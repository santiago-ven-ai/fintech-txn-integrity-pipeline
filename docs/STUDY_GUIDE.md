# Study Guide — fintech-txn-integrity-pipeline

Esta guía es para **entender** el repo a fondo, no para correrlo (eso es
[`RUNBOOK.md`](RUNBOOK.md)) ni para construirlo desde cero (eso es
[`LEARNING_BUILD.md`](LEARNING_BUILD.md)). El objetivo: poder explicar cada
componente, cómo interactúan, qué pasa cuando algo falla, y defender cada
número del README en una entrevista técnica.

Todas las rutas de archivo son relativas a la raíz del repo. Todas las
métricas citadas están en la tabla "Measured in this repo" del `README.md` —
si un número no está ahí, no lo repitas como medido.

---

## 1. Resumen en 3 niveles

**Una frase:** Pipeline de ingesta exactly-once para transacciones de pago,
para que un reintento del productor nunca liquide dos veces la misma
transacción.

**30 segundos (EN):** *"An exactly-once transaction ingestion pipeline for
payment platforms. A Go gate does a single conditional DynamoDB write per
idempotency key — first write wins, every retry gets a 409 with zero
side effects. Everything downstream (schema validation, Parquet
compaction, the outbox) is at-least-once by design, because only the gate's
conditional write needs to be exact."*

**2 minutos (EN):** *"Producers retry on ambiguous ack failures — that's
normal and expected. The problem is a payments ledger where a retried
transaction settles twice. I built a Go/Gin gate that does one thing: a
DynamoDB PutItem with ConditionExpression attribute_not_exists on the
idempotency key. First write wins with a 200, every subsequent write with
the same key fails the condition and gets a 409 with no trace left behind.
That's the one synchronous, latency-critical hop — everything else is async:
SNS/SQS fan-out, schema-versioned validation that quarantines instead of
drops, PySpark compaction into Parquet, and a transactional outbox so a
completion event can never be silently lost after the business fact is
already committed. I measured a 20-way concurrent race on one key — exactly
1 accepted, 19 rejected, every time — and I measured where this design
actually breaks: not at 1 TB like a naive claim would suggest, but much
sooner, because the gate's serial throughput, not Spark, is the real
bottleneck at scale."*

---

## 2. Mapa de componentes

| Componente | Archivo | Responsabilidad | Input → Output | Por qué existe / por qué esa tecnología |
|---|---|---|---|---|
| Generador de eventos | `src/ingestion/data_gen.py` | Sintetiza transacciones con ~8% de reintentos inyectados (misma `idempotency_key`, nuevo `txn_id`, timestamp posterior) | `--count`, `--retry-rate` → `events.jsonl` | Sin duplicados inyectados no hay nada que probar; el bug real que arregla este repo es un problema de reintentos |
| Publisher | `src/ingestion/publisher.py` | Publica cada línea del JSONL a un tópico SNS | JSONL → mensajes SNS | Simula el productor real; SNS permite fan-out a validación y auditoría sin acoplar al productor |
| Idempotency gate | `src/ingestion/gate/main.go` | Único hop síncrono: `PutItem` condicional sobre `idempotency_key`. 200 si gana, 409 si pierde | HTTP POST evento → 200/409 | **Por qué Go:** hot path de baja latencia, stateless, alta QPS — un binario compilado evita el cold-start de Python en el único punto donde la latencia importa de verdad |
| Consumer | `src/ingestion/consumer.py` | Hace polling de la cola de validación, llama al gate, y si acepta corre el validador de schema | Mensaje SQS → llamada al gate → S3 | Es el pegamento entre async (SQS) y el hop síncrono (gate) |
| Validador de schema | `src/models/validator.py` | Verifica `schema_version` contra un registry en S3 y campos requeridos; escribe a `txn-raw` o `txn-quarantine` | Evento aceptado por el gate → objeto S3 con o sin `_quarantine_reason` | Un evento inválido nunca se descarta en silencio — se puede reproducir después de arreglar el productor o el registry |
| Driver de orquestación | `src/orchestration/statemachine.py` | Despliega Lambdas + máquinas de estado, corre pre-flight (Step Functions) → `curate.py` (subproceso) → post-flight (Step Functions) | `job_id` → job completado o fallido en cada etapa | Step Functions orquesta el control de flujo; Spark corre **fuera** de una Lambda porque un runtime de Lambda no puede alojar un cluster Spark — el mismo patrón que EMR/Glue en AWS real |
| Lambda de status | `src/orchestration/lambdas/record_status.py` | Escribe una fila de status por ejecución; en `completed`, escribe también una fila `PENDING` en el outbox, **en la misma transacción** | `{job_id, status}` → filas en `txn-curation-jobs` (+ `txn-outbox` si completed) | El hecho de negocio y el evento pendiente deben comprometerse juntos o ninguno — ver ADR 0002 |
| Outbox publisher | `src/orchestration/outbox_publisher.py` | Proceso aparte que escanea `PENDING` en `txn-outbox`, publica a SNS y marca `PUBLISHED` | Filas `PENDING` → mensajes SNS + filas `PUBLISHED` | Publicar inline en la Lambda reintroduciría la falla que el outbox existe para evitar (ver §4) |
| Compactación PySpark | `src/transformation/curate.py` | Lee JSON chicos de `txn-raw/valid/`, dedup defensivo por `idempotency_key`, escribe Parquet particionado por `ingest_hour` | S3 JSON → S3 Parquet | Muchos archivos JSON chicos son caros de listar/leer; Parquet particionado es lo que consulta el warehouse |
| Curación incremental | `src/transformation/curate_incremental.py` | Alternativa opt-in: batches acotados contra una tabla DynamoDB persistente en vez de un shuffle global | Batches de 100K filas → Parquet + `txn-curated-keys` | Aplica el mismo principio "sin shuffle" del gate a la capa batch — ver §7 escala |
| Warehouse | `src/utils/warehouse.py` | DuckDB leyendo Parquet directo de S3 vía `httpfs`, como stand-in de Redshift | Consulta SQL → filas | Mismo patrón de acceso que `COPY`/Spectrum de Redshift, sin correr un cluster MPP real |
| API de serving | `src/serving/api.py` | FastAPI: estado de una transacción, métrica de duplicados, métrica de SLA | HTTP GET → JSON | Expone lo que un operador o un dashboard necesitaría consultar en producción |

---

## 3. Recorrido de un evento (el camino feliz y el de duplicado)

```
data_gen.py → events.jsonl (8% son duplicados con misma idempotency_key)
      |
      v
publisher.py → SNS topic "txn-events"
      |
      +---------------------------+
      v                           v
SQS "txn-validation-queue"   SQS "txn-audit-queue" → DLQ "txn-audit-dlq"
      |
      v
consumer.py --POST /accept--> gate (Go) :8080
      |
      +-- PRIMERA VEZ: PutItem condicional OK -> 200
      |        |
      |        v
      |   validator.py revisa schema_version + campos
      |        |
      |    +---+---+
      |    v       v
      |  válido  inválido
      |    |       |
      |    v       v
      | S3 txn-raw  S3 txn-quarantine (_quarantine_reason)
      |
      +-- REINTENTO: PutItem condicional falla (ConditionalCheckFailedException)
               -> 409, NO se escribe en S3, se cachea la key en el LRU
               -> se incrementa duplicate_rejections en txn-gate-metrics

(diariamente, disparado por statemachine.py)
Step Functions preflight -> record_status.py (status=started)
      |
      v
curate.py (PySpark): dedup defensivo + Parquet particionado por ingest_hour
      |
      v
Step Functions postflight -> record_status.py (status=completed)
      |   dentro de la MISMA transact_write_items:
      |   fila de status + fila PENDING en txn-outbox
      v
outbox_publisher.py (proceso aparte, se corre cuando se corre)
      -> SNS "txn-curation-events" + marca PUBLISHED

api.py :: GET /txn/{key}         lee de S3 (raw o quarantine)
api.py :: GET /metrics/dedup     lee contadores atómicos de txn-gate-metrics
api.py :: GET /metrics/sla       consulta DuckDB sobre el Parquet curado
```

**El punto que hay que poder explicar sin dudar:** un duplicado rechazado
por el gate **no deja rastro** en `txn-idempotency` más allá del original —
por eso `txn-gate-metrics` existe como contador aparte (`/metrics/dedup` en
`api.py` lo explica en su propio docstring): si contaras rechazos a partir de
la tabla de idempotencia, siempre darían cero.

---

## 4. Interacciones y contratos entre componentes

| Frontera | Garantía | Por qué |
|---|---|---|
| `POST /accept` (gate) | **Exactly-once** | Un único `PutItem` con `ConditionExpression: attribute_not_exists(idempotency_key)` — la escritura condicional ES el detector de duplicados, no hay lectura-luego-escritura |
| `POST /accept/batch` (gate) | **At-least-once**, explícitamente | `BatchGetItem`/`BatchWriteItem` no soportan `ConditionExpression`; dos batches concurrentes con la misma key nueva pueden ambos reportar "accepted". Es un trade-off documentado, no un bug — se usa cuando el volumen importa más que la garantía atómica |
| SNS → SQS → gate | At-least-once (SQS puede redeliverar) | El gate es quien convierte "at-least-once en la cola" en "exactly-once en el negocio" — por diseño, no por casualidad |
| `record_status.py` → `txn-outbox` | Atómico (una sola `transact_write_items`) | El hecho de negocio y el evento pendiente se comprometen juntos o ninguno — ver ADR 0002 |
| `outbox_publisher.py` → SNS | At-least-once, idempotente | Si el proceso muere entre publicar y marcar `PUBLISHED`, el siguiente run vuelve a publicar. Un duplicado en `txn-curation-events` es preferible a perder el evento en silencio |
| `curate.py` reprocesado | Determinista | `tests/data_quality/test_e2e.py::curate_reprocess_same_row_count` verifica que re-correr sobre el mismo input crudo da el mismo `rows_out` |

---

## 5. Modos de falla — "¿qué pasa si...?"

| Si esto pasa... | ...entonces | Dónde se ve / se prueba |
|---|---|---|
| El productor reenvía la misma `idempotency_key` | El gate responde 409, no escribe S3, incrementa `duplicate_rejections` | `tests/integration/test_idempotency.py` |
| 20 requests concurrentes con la misma key nueva llegan al mismo tiempo a `/accept` | Exactamente 1 gana (200), 19 pierden (409) — nunca 2 ganan | `tests/integration/test_chaos.py::test_concurrent_duplicate_requests_only_one_wins` |
| 20 requests concurrentes con la misma key nueva llegan a `/accept/batch` | Puede haber más de un "accepted" reportado — ventana de carrera conocida y aceptada | Docstring de `acceptBatchHandler` en `main.go`; verificado manualmente (18/20 "accepted" en una corrida) |
| Un evento tiene `schema_version` viejo o `amount_cents` inválido | Va a `txn-quarantine` con `_quarantine_reason`, nunca se descarta ni se fuerza | `src/models/validator.py::validate_event` |
| El flush de métricas del gate falla | Los contadores no se pierden — se re-suman al acumulador en memoria para el próximo intento (puede sobre-contar levemente si el `UpdateItem` sí llegó a escribir del lado del servidor) | `flushMetrics` en `main.go` |
| `outbox_publisher.py` nunca se corre | Las filas `PENDING` simplemente se acumulan en `txn-outbox`; nada se rompe, nada se pierde | Docstring de `outbox_publisher.py` |
| `sns.publish()` se llamara directo dentro de `record_status.py` (el bug que el outbox evita) | Si el publish falla justo después del `put_item`, el job queda marcado `completed` en DynamoDB pero el evento nunca se publicó — pérdida silenciosa | Explicado en el docstring de `record_status.py` y en ADR 0002 |
| Se re-corre `curate.py` sobre el mismo raw | `rows_out` debe ser idéntico | `tests/data_quality/test_e2e.py` |
| El gate recibe tráfico por encima de su capacidad serial (~273 req/s de un solo caller) | Un caller serial se satura; la capacidad *concurrente* real es ~826 req/s a 16 workers — la distinción entre "capacidad de un caller" y "capacidad del servicio" es la que casi nadie mide | `scripts/bench.py::bench_gate_saturation_curve`, ver §7 |
| Se necesitara escalar la curación a 1 TB real | El shuffle global de `curate.py` no cabe (proyectado ~420 GB de spill contra 50 GB de disco libre) — la respuesta no es "más Spark", es batches acotados (`curate_incremental.py`) y, sobre todo, batchear el gate, que es el cuello de botella real, no Spark | `docs/scale-roadmap.md`, ver §7 |

---

## 6. Conceptos senior — qué son, cómo aparecen aquí, cuándo NO usarlos

### Conditional write como mecanismo de idempotencia
**Qué es:** usar la atomicidad de una escritura condicional de la base de
datos como el propio detector de duplicados, en vez de una lectura seguida
de una escritura.
**Aquí:** `ConditionExpression: attribute_not_exists(idempotency_key)` en
`main.go::acceptHandler`.
**Trade-off:** una sola escritura atómica, sin lock separado, sin segundo
failure mode (un lock que queda tomado tras un crash).
**Cuándo NO usarlo:** cuando necesitas coordinar más de un recurso a la vez
(ahí sí necesitas un lock distribuido o una transacción de verdad) o cuando
el motor de datos no ofrece escrituras condicionales atómicas.
**Cómo escalaría en producción real:** igual — DynamoDB condicional escala
horizontalmente sin cambios; el límite real sería el throughput de
escrituras por partición, no el patrón en sí.

### Transactional outbox
**Qué es:** comprometer el hecho de negocio y el evento-a-publicar en la
misma transacción atómica, y publicar desde un proceso aparte que lee esa
tabla.
**Aquí:** `record_status.py` escribe status + `txn-outbox` en un
`transact_write_items`; `outbox_publisher.py` publica y marca `PUBLISHED`.
**Trade-off:** at-least-once hacia el consumidor final (puede llegar
duplicado); a cambio, nunca se pierde en silencio.
**Cuándo NO usarlo:** si perder ocasionalmente un evento de baja importancia
es aceptable y nadie va a re-conciliar — el patrón añade una tabla, un
publisher y complejidad operativa que solo se justifica si hay un lector
real que depende de la garantía.
**Cómo escalaría:** en AWS real, con DynamoDB Streams o CDC (Debezium) en
vez de un `scan()` periódico — el propio ADR 0002 dice explícitamente que
MiniStack no soporta streams confiables aquí, así que el poller es la opción
honesta para este entorno, no la opción ideal para producción a escala.

### Schema evolution con cuarentena
**Qué es:** versionar el schema explícitamente y desviar lo que no calza a
un lugar reproducible, en vez de fallar duro o descartar en silencio.
**Aquí:** `schema_version` contra un registry en S3; inválidos van a
`txn-quarantine` con `_quarantine_reason`.
**Trade-off:** más piezas (registry, bucket de cuarentena) contra la
capacidad de hacer replay después de arreglar el productor.
**Cuándo NO usarlo:** pipelines internos de un solo equipo donde el schema
nunca cambia sin coordinación directa — la cuarentena es para cuando el
productor y el consumidor pueden desincronizarse.

### Compactación de archivos chicos
**Qué es:** fusionar muchos objetos JSON pequeños en pocos archivos Parquet
grandes, particionados, para bajar el costo de listar/leer y mejorar el
tiempo de consulta.
**Aquí:** `curate.py`, dedup defensivo + `partitionBy("ingest_hour")`.
**Trade-off:** latencia (el compactado es un job diario, no en tiempo real)
por eficiencia de consulta y costo de S3.
**Cuándo NO usarlo:** si el patrón de consulta es siempre "lee el evento más
reciente por key" y nunca un scan analítico — ahí compactar no ayuda.

### Curva de escala medida, no una afirmación de "probado a 1 TB"
**Qué es:** medir el comportamiento real a volúmenes crecientes y
extrapolar con supuestos explícitos, en vez de afirmar una escala nunca
alcanzada.
**Aquí:** `scripts/scale_bench.py` mide hasta 10M filas reales y extrapola a
1 TB — y el propio reporte dice **dónde la extrapolación deja de ser
válida** (proyectado ~420 GB de shuffle contra 50 GB de disco libre).
**Por qué importa en entrevista:** decir "no lo probé a 1 TB, y esto es
exactamente por qué, y esto es lo que SÍ medí" es más creíble que una
afirmación vaga de "escala a producción".
**El hallazgo más senior del repo:** el cuello de botella real a esa escala
no es Spark, es el gate síncrono (~273 req/s serial ⇒ ~196 días para 1 TB
solo por el gate, contra ~12 horas del lado Spark). La solución no es
"más Spark", es batchear el chequeo de idempotencia.

### Serial throughput vs. capacidad concurrente real
**Qué es:** la diferencia entre medir "cuántas requests por segundo puede
enviar un solo caller secuencial" y "cuántas puede absorber el servicio bajo
concurrencia real".
**Aquí:** el gate parecía limitado a ~273 req/s (medición serial) hasta que
`bench_gate_saturation_curve()` midió la capacidad concurrente real: ~826
req/s a 16 workers. El fix real fue sacar las métricas del request path
(contadores atómicos + flush periódico) y un LRU cache para 409 conocidos.
**Por qué importa:** un número "lento" puede ser un artefacto de cómo se
midió, no del sistema — vale la pena dudar del propio benchmark antes de
optimizar a ciegas.

---

## 7. ADRs en 3 líneas

- **ADR 0001 — PutItem condicional vs. check-then-write vs. lock distribuido
  vs. SQS FIFO dedup:** se eligió el `PutItem` condicional porque check-then-
  write tiene una carrera real y un lock añade latencia y un failure mode
  nuevo para un recurso que ya es atómico. Restricción real: SQS FIFO
  dedup es dedup de cola, no de negocio — un reintento con nuevo
  `MessageDeduplicationId` igual asentaría dos veces.
- **ADR 0002 — Outbox transaccional vs. dual-write vs. CDC:** dual-write
  puede perder el evento en silencio si el publish falla después del write;
  CDC/Debezium sería correcto en AWS real pero MiniStack no ofrece streams
  confiables aquí, así que el outbox con poller es la opción honesta para
  este entorno.
- **ADR 0003 — Cuarentena en S3 vs. drop silencioso vs. rechazo en el
  gate:** rechazar en el gate mezclaría idempotencia con validación de
  schema, y el gate debe seguir siendo un hop de milisegundos — el schema
  vive en Python junto al registry.

---

## 8. Métricas y cómo defenderlas

| Métrica publicada | Cómo se midió | Límite honesto que hay que decir sin que lo pregunten |
|---|---|---|
| 0 duplicados asentados; 1 aceptado / 19 rechazados en carrera de 20 | `tests/integration/test_chaos.py`, corrida real contra MiniStack | Es una carrera de 20 sobre una sola key en una máquina — no un load test de producción |
| Gate p95 2.87 ms / mean 2.44 ms (200 requests) | `make bench` → `benchmarks/results.json` | Es *serial*, un solo caller — no es la capacidad del servicio |
| ~826 req/s a 16 workers concurrentes | `make bench-gate-concurrent` → `benchmarks/gate-throughput.json` | Es la capacidad medida en esta máquina, no un SLA de producción |
| Duplicate rate 0.10 sobre 10% inyectado | `scripts/bench.py` tras un `data_gen.py` + replay limpio | Coincide con la tasa inyectada — es la prueba de que el gate no genera falsos positivos/negativos, no una tasa real de mercado |
| 191/191 filas preservadas en curate | Corrida real de `curate.py` | Volumen de demo, no de producción |
| ~$1.9M/yr evitado | `docs/impact-model.md` | Es un **modelo** con supuestos parcialmente sin citar (ver el TODO en el propio archivo) — nunca decir que es un ahorro medido |
| Extrapolación a 1 TB: ~11.6h, pero rota antes de llegar | `scripts/scale_bench.py`, `docs/scale-report.md` | Es una extrapolación lineal desde 10M filas, explícitamente declarada como tal, y el propio reporte dice dónde deja de ser físicamente posible en esta máquina |

---

## 9. Preguntas de entrevista (EN)

1. **"Walk me through what happens when a duplicate transaction arrives."**
   Gate receives the POST, issues a conditional `PutItem` on
   `idempotency_key`. First arrival: write succeeds, 200, event flows to
   validation and S3. Retry: the condition fails
   (`ConditionalCheckFailedException`), gate returns 409, no S3 write
   happens, and a separate atomic counter in `txn-gate-metrics` increments —
   because a failed conditional write leaves no trace in the idempotency
   table itself.

2. **"Why a conditional write instead of a distributed lock?"**
   A lock adds a second failure mode (a lock held past a crash) for no
   benefit over a write that's already atomic at the database layer. Also
   rules out check-then-write's real race window.

3. **"What guarantee does `/accept/batch` NOT give you, and why does it
   still exist?"**
   It trades `/accept`'s exact-one-winner guarantee for far fewer
   round-trips — `BatchGetItem`/`BatchWriteItem` don't support
   `ConditionExpression`, so two concurrent batches with the same brand-new
   key can both report "accepted." It exists for high-volume ingestion
   where at-least-once is an acceptable trade; `/accept` is unchanged for
   when the atomic guarantee matters.

4. **"What's the transactional outbox pattern, and what bug does it
   prevent?"** Committing the business fact (job completed) and the
   not-yet-published event in one atomic transaction, then publishing from
   a separate idempotent process. It prevents a lost event: if you
   published inline right after the write and the publish call failed, the
   job would be marked completed with the event silently gone.

5. **"Why is the outbox publisher a separate process instead of being
   inlined in the Lambda?"** Because inlining it reintroduces exactly the
   failure mode the pattern exists to prevent — see #4.

6. **"Have you tested this at terabyte scale?"**
   No, and I can explain exactly why not instead of guessing: 1 TB of these
   237-byte events is ~4.6 billion rows, and this machine has 50 GB of free
   disk. I measured the real dedup path up to 10 million rows (110,761
   rows/s, zero shuffle spill), extrapolated linearly to 1 TB (~11.6
   hours), and then checked whether that extrapolation is physically
   possible here — it isn't, because the projected shuffle volume at that
   scale is ~420 GB, 8x the free disk.

7. **"So what actually is the bottleneck at scale?"**
   Not Spark — the gate. Its measured serial throughput (~273 events/s)
   would take ~196 days to process 1 TB of events, versus ~12 hours for the
   Spark side. Scaling this pipeline means batching the idempotency check,
   not tuning Spark.

8. **"You said 826 req/s at 16 workers, but earlier docs said ~273 req/s —
   which is it?"** Both are true and measure different things: 273 is a
   single serial caller's throughput; nobody had measured the gate's real
   concurrent capacity until I built a saturation-curve benchmark. The fix
   that got us there: moving metrics off the request path (atomic counters
   + periodic flush) and an LRU cache for known-duplicate responses.

9. **"Why quarantine instead of dropping invalid events?"**
   A dropped event is unrecoverable — a schema bump would silently empty
   the ledger with no alarm. Quarantine with an explicit reason makes
   replay possible once the registry or producer is fixed.

10. **"Why is schema validation in Python and not in the Go gate?"**
    The gate has to stay a single-responsibility, millisecond-scale hop.
    Mixing idempotency and schema validation there would slow down the one
    part of the pipeline where latency is the whole point.

11. **"How do you know your reprocessing is safe / deterministic?"**
    `tests/data_quality/test_e2e.py` reruns `curate.py` against the same
    raw input and checks `rows_out` is identical — it's a test, not an
    assumption.

12. **"Is the $1.9M/year number real?"**
    No — it's a stated model (8M transactions/month, 0.4% duplicate
    settlement rate without dedup, from a public benchmark) documented in
    `docs/impact-model.md`, not a measured production result. I keep those
    two things labeled separately on purpose.

13. **"Why DynamoDB for idempotency instead of Postgres?"**
    Single-digit-millisecond conditional writes at exactly the access
    pattern needed (point lookup by key), no connection pool to manage from
    a stateless handler, and it's the same service the rest of the pipeline
    already depends on.

14. **"What would you change to run this against real AWS tomorrow?"**
    Nothing in the application code — `boto3`'s `endpoint_url` and the AWS
    CLI's `AWS_ENDPOINT_URL` are the only MiniStack-specific bits, and
    they're one environment variable. I'd turn on real IAM enforcement
    (an account setting, not code) and add CloudWatch alarms on outbox
    `PENDING` row age.

15. **"What's the honest limitation of your IAM setup?"**
    MiniStack accepts and stores real least-privilege policies and
    correctly evaluates `iam simulate-principal-policy`, but doesn't
    enforce them on live calls — I verified this myself (a role with an
    explicit `Deny *` could still call `s3 ls`). I built the exercise
    around the tool that does work correctly instead of pretending
    enforcement works.

---

## 10. Flashcards

<!-- card -->
Q: ¿Qué garantía exacta da `POST /accept` y con qué mecanismo?
A: Exactly-once, vía `PutItem` con `ConditionExpression: attribute_not_exists(idempotency_key)` sobre DynamoDB.

<!-- card -->
Q: ¿Por qué `/accept/batch` no da la misma garantía que `/accept`?
A: `BatchGetItem`/`BatchWriteItem` no soportan `ConditionExpression`; es lectura-luego-escritura en dos fases, con ventana de carrera documentada.

<!-- card -->
Q: ¿Qué pasa con un evento cuyo `schema_version` no coincide con el registry?
A: Va a `txn-quarantine` con `_quarantine_reason`, nunca se descarta silenciosamente.

<!-- card -->
Q: ¿Por qué el outbox publisher es un proceso separado de `record_status.py`?
A: Porque publicar inline reintroduciría la falla que el outbox existe para prevenir: un publish fallido después de un write exitoso perdería el evento en silencio.

<!-- card -->
Q: ¿Qué tabla guarda los contadores de duplicados rechazados, y por qué no se pueden inferir de `txn-idempotency`?
A: `txn-gate-metrics`. Un `PutItem` condicional fallido no deja ninguna fila en `txn-idempotency` — no hay nada ahí de qué contar.

<!-- card -->
Q: ¿Por qué está el gate escrito en Go y no en Python?
A: Es el único hop síncrono de baja latencia y alta QPS del pipeline; un binario compilado evita el cold-start de Python justo donde la latencia importa.

<!-- card -->
Q: ¿Cuál es el verdadero cuello de botella del pipeline a escala de 1 TB?
A: El gate síncrono (~273 eventos/s serial ⇒ ~196 días para 1 TB), no Spark (~12 horas para el mismo volumen).

<!-- card -->
Q: ¿Por qué se particiona por un string formateado (`ingest_hour`) y no por la columna timestamp directa?
A: Particionar directo sobre un `TimestampType` dispara un bug de generación de rutas en el S3A committer de hadoop-aws 3.5.0.

<!-- card -->
Q: ¿Qué distingue la medición "serial" de la "concurrente" del throughput del gate?
A: Serial mide un solo caller secuencial (~273 req/s); concurrente mide la capacidad real del servicio bajo carga simultánea (~826 req/s a 16 workers) — son números distintos, no contradictorios.

<!-- card -->
Q: ¿Qué prueba que la re-curación (`curate.py`) es determinista?
A: `tests/data_quality/test_e2e.py::curate_reprocess_same_row_count` — mismo `rows_out` al reprocesar el mismo raw input.

<!-- card -->
Q: ¿Qué opción se descartó para exactly-once y por qué?
A: Check-then-write (race real entre dos accepts concurrentes) y lock distribuido (latencia extra y un failure mode nuevo, el lock leak).

<!-- card -->
Q: ¿Qué alternativa a la cuarentena en S3 se descartó y por qué?
A: Rechazar en el gate — mezclaría idempotencia con validación de schema y rompería el hop de milisegundos que el gate debe ser.

<!-- card -->
Q: ¿Qué encontró el benchmark de escala sobre la extrapolación lineal a 1 TB?
A: Que rompe antes de llegar: el shuffle proyectado (~420 GB) excede 8x el disco libre (50 GB) de la máquina — no es "sería lento", es "no cabe".

<!-- card -->
Q: ¿Qué limitación honesta tiene el setup de IAM en este repo?
A: MiniStack acepta y guarda roles/políticas reales y evalúa bien `simulate-principal-policy`, pero no las enforce en llamadas en vivo — verificado con un `Deny *` que igual dejó pasar `s3 ls`.

<!-- card -->
Q: ¿Es el número de $1.9M/yr una cifra medida?
A: No — es un modelo (`docs/impact-model.md`) con supuestos declarados, algunos sin cita pública todavía (marcado TODO en el propio archivo).

<!-- card -->
Q: ¿Qué hace `curate_incremental.py` distinto de `curate.py`?
A: Reemplaza el shuffle global por batches acotados verificados contra una tabla DynamoDB persistente (`txn-curated-keys`) — el mismo principio "sin shuffle" que ya usa el gate, aplicado a la capa batch.

<!-- card -->
Q: ¿Por qué Redshift está representado por DuckDB en este repo?
A: DuckDB lee el mismo Parquet particionado directo de S3 vía `httpfs`, el mismo patrón de acceso que `COPY`/Spectrum de Redshift, sin correr un cluster MPP real.

<!-- card -->
Q: ¿Qué SLA/contrato tiene SNS→SQS→gate y quién lo convierte en exactly-once?
A: SNS/SQS son at-least-once (SQS puede redeliverar); el gate es el componente que convierte eso en exactly-once de negocio.

<!-- card -->
Q: ¿Qué error de bug real se encontró construyendo el generador de reintentos?
A: `row_id % n_orig` era un mapeo identidad para el 92% de las filas, así que solo ~0.8% terminaba siendo duplicado en vez del 8% pretendido — se corrigió eligiendo un target aleatorio entre filas anteriores.

<!-- card -->
Q: ¿Qué significa que el LRU del gate "solo acelera" el camino de duplicado?
A: Un hit en caché significa que esa key ya tuvo un `PutItem` exitoso en algún momento — es seguro responder 409 sin ir a DynamoDB. Un miss (incluida cualquier key evictada) siempre cae al `PutItem` real, así que la eviction nunca permite que un duplicado real se acepte.

---

## 11. Quiz

<!-- quiz -->
Q: Un reintento con la misma `idempotency_key` llega al gate después de que el primero fue aceptado. ¿Qué responde el gate?
- [ ] 200, y sobreescribe la fila existente
- [x] 409, sin escribir en DynamoDB ni en S3
- [ ] 500, porque la clave ya existe
- [ ] 202, y lo encola para revisión manual

Why: `ConditionExpression: attribute_not_exists(idempotency_key)` hace que el segundo `PutItem` falle limpio con `ConditionalCheckFailedException`, que el handler traduce a 409 — sin efectos secundarios.

<!-- quiz -->
Q: ¿Por qué `POST /accept/batch` puede aceptar dos veces la misma key nueva bajo concurrencia?
- [ ] Es un bug no documentado
- [ ] Usa una tabla DynamoDB distinta
- [x] `BatchGetItem`/`BatchWriteItem` no soportan `ConditionExpression`, así que es lectura-luego-escritura en dos fases
- [ ] Porque no usa el LRU cache

Why: Es un trade-off documentado en el código y en el ADR 0001, no un bug oculto — se acepta at-least-once a cambio de mucho menos round-trips.

<!-- quiz -->
Q: ¿Qué pasa si `sns.publish()` se llamara directamente dentro de `record_status.py` en vez de usar el outbox?
- [ ] Nada distinto, es equivalente
- [x] Un publish fallido después de un write exitoso perdería el evento en silencio, con el job ya marcado como completado
- [ ] La transacción de DynamoDB haría rollback automáticamente
- [ ] SNS reintentaría indefinidamente hasta lograrlo

Why: Ese es exactamente el failure mode que el patrón de outbox transaccional existe para prevenir — la escritura y el evento pendiente se comprometen atómicamente, y un proceso aparte se encarga de publicar con reintentos seguros.

<!-- quiz -->
Q: El benchmark de escala mide 110,761 filas/s hasta 10M filas y extrapola linealmente a 1 TB (~11.6h). ¿Qué dice el propio reporte sobre esa extrapolación?
- [x] Que se rompe antes de llegar a 1 TB porque el shuffle proyectado excede varias veces el disco libre de la máquina
- [ ] Que es exacta y se puede confiar en ella sin reservas
- [ ] Que el cuello de botella sería la red, no el disco
- [ ] Que no aplica porque Spark nunca hace shuffle en este job

Why: El shuffle proyectado a 1 TB (~420 GB) es 8x el disco libre (50 GB) — la conclusión honesta es que el pipeline "no cabe" a esa escala en esta máquina, no solo que "sería lento".

<!-- quiz -->
Q: ¿Cuál es el verdadero cuello de botella del pipeline completo a volumen de 1 TB, según la medición?
- [ ] La compactación PySpark
- [ ] El publisher del outbox
- [x] El gate síncrono, por su throughput serial medido (~273 eventos/s)
- [ ] El validador de schema en S3

Why: A ese throughput, procesar 1 TB tomaría ~196 días solo en el gate, contra ~12 horas del lado Spark — escalar de verdad significa batchear el chequeo de idempotencia, no optimizar Spark.

<!-- quiz -->
Q: ¿Por qué `txn-gate-metrics` es una tabla separada de `txn-idempotency`?
- [ ] Por límites de tamaño de DynamoDB
- [x] Porque un `PutItem` condicional fallido no deja ninguna fila en `txn-idempotency`, así que no hay nada ahí de qué contar duplicados
- [ ] Porque el gate no tiene permiso de escritura en `txn-idempotency`
- [ ] Es una decisión arbitraria sin razón técnica

Why: El docstring de `dedup_metrics()` en `api.py` lo explica directamente — contar rechazos desde la tabla de idempotencia siempre daría cero.

<!-- quiz -->
Q: ¿Qué alternativa a la cuarentena en S3 se rechazó explícitamente en el ADR 0003, y por qué?
- [x] Rechazar en el gate Go, porque mezclaría idempotencia con validación de schema y rompería su latencia de milisegundos
- [ ] Escribir a una cola SQS de reintentos automáticos
- [ ] Usar un segundo gate en Python
- [ ] Ignorar el `schema_version` completamente

Why: El gate debe seguir siendo un hop de un solo propósito; el schema vive en Python junto al registry en S3, no en el hot path.

<!-- quiz -->
Q: Según el `impact-model.md`, ¿qué tan confiable es el número de "$1.9M/yr evitado"?
- [ ] Es una cifra medida en producción real
- [x] Es un modelo con supuestos explícitos, algunos aún sin cita pública (marcados TODO)
- [ ] Es un promedio de la industria fintech verificado por un tercero
- [ ] No tiene ninguna documentación de cómo se calculó

Why: El propio archivo dice que nunca se debe cambiar esa cifra sin actualizar el modelo, y que la respuesta a "¿cómo llegaste a este número?" debe ser "lee `docs/impact-model.md`", no una estimación de memoria.

<!-- quiz -->
Q: ¿Qué demuestra la medición separada de "serial ~273 req/s" vs. "concurrente ~826 req/s a 16 workers"?
- [x] Que nadie había medido la capacidad real del gate bajo concurrencia hasta construir el benchmark de saturación — el número serial subestimaba la capacidad real del servicio
- [ ] Que el gate tiene un bug de concurrencia que hace que procese más rápido de lo posible
- [ ] Que las dos mediciones son incompatibles y una está mal
- [ ] Que DynamoDB es más rápido bajo carga baja

Why: Son mediciones de cosas distintas — throughput de un caller secuencial vs. capacidad real del servicio — y ambas son válidas y coexisten en el README.

<!-- quiz -->
Q: ¿Qué principio comparten `curate_incremental.py` y el gate Go?
- [x] Evitar un shuffle/lectura global usando un chequeo acotado contra un almacén persistente (DynamoDB) en cada batch/request
- [ ] Ambos están escritos en Go
- [ ] Ambos corren dentro de una Lambda
- [ ] Ambos ignoran duplicados en vez de rechazarlos

Why: El gate chequea una key a la vez contra DynamoDB en tiempo real; `curate_incremental.py` aplica el mismo principio de "sin shuffle global" a la capa batch, chequeando cada batch acotado contra `txn-curated-keys`.
