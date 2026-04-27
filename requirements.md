# Application Requirements

## Assumptions

### Platform & Build
- Java / Spring Boot application built with Gradle
- Deployable as 10 concurrent instances on a Kafka cluster

### Message Structure
- Every message is a JSON envelope with two top-level properties:
  - **`event`** — header/metadata object containing:
    - `interactionId` — unique identifier for the interaction; used as the correlation ID for all logging
    - `eventType` — the type/category of the event; see known event types below
    - `backdated` — optional boolean flag; when `true` combined with `eventType: END`, the message is a **Backdated Endorsement**
  - **`body`** — payload object containing:
    - `messageId` — unique identifier for the message; must be a valid UUID (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`); messages with a missing or non-UUID `messageId` are routed to dead letter with reason code `INVALID_MESSAGE_ID`
- Both `event` and `body` must be deserialized into strongly-typed Java objects
- `interactionId` must be propagated through the entire processing lifecycle (logging, control records, dead letter)

### Event Types

| Code | Name | Notes |
|------|------|-------|
| `NC`  | New Business | Standard new policy intake |
| `END` | Endorsement | Policy modification; if `backdated=true` this is a **Backdated Endorsement** — siphoned directly to `kafka.topic.siphon-bde` on the consumer thread, bypassing all processing |
| `TRM` | Termination | Policy cancellation/termination |
| `RNW` | Renewal | Policy renewal |

### Throughput & Scalability
- Must sustain a minimum of 1,000,000 messages per day (~12 msg/sec average; must handle burst capacity above this)
- Concurrency and thread pool sizes must be configurable via application properties

### Kafka Reading
- No duplicate messages may be processed; each `messageId` must be handled exactly once
- Deduplication is enforced via **Kafka exactly-once semantics (EOS)**:
  - Consumer is configured with `isolation.level=read_committed` — only reads messages from committed transactions
  - Consumer is configured with `auto.offset.reset=earliest` — when a consumer group starts with no committed offset (first run, new group, or topic recreated), it reads from the beginning of the topic rather than skipping existing messages
  - Producer is configured as a **transactional producer** with a unique `transactional-id` per instance
  - The consumer-to-producer flow operates within a single Kafka transaction — the input read and output publish either both commit or both roll back
- If a duplicate `messageId` is detected despite EOS (e.g., a replayed message), it must be routed to the Dead Letter Component with reason code `DUPLICATE`
- Duplicate detection uses a **two-layer approach** to keep the consumer thread free of DB round-trips:
  1. **In-memory gate (consumer thread)** — `ConcurrentHashMap.newKeySet()` holds all in-flight `messageId`s; `add()` returns `false` if already present, making in-flight duplicate detection a nanosecond operation with zero DB cost. The set is cleared on restart.
  2. **DB unique constraint (worker thread)** — `ReceivedRecord` has a unique constraint on `message_id`. If the constraint fires (`DataIntegrityViolationException`) during `recordReceived()` on the worker thread, it means the app restarted mid-flight (in-memory state lost, row survived). Route to dead letter + ack.
- The in-flight set is removed from in the worker's `finally` block — on success (allows explicit replay) and on failure (allows redelivery after no-ack)
- To replay a failed message: delete its `ReceivedRecord` row, then replay the Kafka message; the constraint insert will succeed and the message will be processed normally

## Capabilities

### Kafka Consumer
- Listens to a configured Kafka input topic
- Uses a shared, common `groupId` across all 10 instances for coordinated partition consumption
- Manual acknowledgment (`enable.auto.commit=false`) — offsets are committed only after successful downstream publish
- Processes messages concurrently (multi-threaded / async processing per message)
- Consumer thread count per instance is configurable via `kafka.consumer.concurrency`
- The correct value is `partitions ÷ instances` — e.g., 10 partitions across 10 instances = `concurrency: 1`; setting it higher wastes idle threads

### Message Processing
- Message payloads are JSON; deserialization must be handled on ingest
- Upon successful deserialization, the message is checked for a **BDE event type siphon** before any other processing
- If `event.eventType == "END"` and `event.backdated == true` (Backdated Endorsement), the message is published as-is to `kafka.topic.siphon-bde` and acked immediately; it **bypasses the duplicate gate and processing pipeline entirely**. A siphon publish failure returns without ack (triggers redelivery).
- The siphon routing system is extensible: additional evaluators can be added by implementing the `SiphonEvaluator` interface (methods: `String eventCode()` and `Optional<String> evaluate(KafkaMessage)`) and registering them as `@Component`. Active evaluators are controlled by `app.siphon.enabled` (list of event codes; empty = all active). The consumer uses a `List<SiphonEvaluator>` bean (filtered by active codes) — first match wins.
- When the consumer thread receives a non-BDE message it first checks the **in-memory in-flight set** (`ConcurrentHashMap`) for the `messageId`; if already present it is a duplicate — route to DUPLICATE dead letter and ack. If not present, the ID is added to the set, the semaphore is acquired (if `app.processing.max-in-flight > 0`), and the work is dispatched for execution via `processingScheduler.execute()`. No DB call happens on the consumer thread.
- When the worker thread fires and processing actually begins, `ControlService.recordReceived()` is called first (DB INSERT with unique constraint), then processing proceeds — `RECEIVED` represents "processing started"; a `ReceivedRecord` with no matching `PublishedRecord` in reconciliation jobs indicates a genuine processing failure
- Each message is processed independently and in a concurrent fashion
- After successful processing, the message is serialized back to JSON and published to a configured Kafka output topic
- Upon successful publish, the Control Component is invoked to write a `PUBLISHED` control record
- Offset commit to the input topic is performed only after the output publish and control record write complete successfully
- Any failure at any stage (deserialization, invalid messageId, processing, publish, or control record write) is handed off to the Dead Letter Component
- The consumer thread dispatches work immediately via `processingScheduler.execute()` — there is no consumer-side scheduling delay; the consumer thread is never blocked waiting for a delay to expire
- Inside the worker thread, an optional configurable sleep (`app.processing.processor-delay-ms`, default `20000`) runs before business logic executes, to account for upstream race conditions; set to `0` to disable
- Each worker has a hard timeout (`app.processing.processor-timeout-ms`, default `10000`) — if a worker thread exceeds this limit the message is cancelled and routed to dead letter with reason code `TIMEOUT`
- Back-pressure is provided by a `Semaphore` (`app.processing.max-in-flight`, default `0` = unlimited) — when the limit is reached the consumer thread blocks on `acquire()` until a worker slot frees up, naturally throttling Kafka polling without pause/resume machinery
- The number of dispatch threads in the scheduler is configurable via `app.processing.worker-threads` (default `32`); these are platform threads used internally by the `ScheduledThreadPoolExecutor` for the delay queue; actual task execution uses virtual threads

### Control Records
- A dedicated **Control Component** is responsible for persisting control records to a database
- Use an in-memory database (e.g., H2) for the initial implementation; the data source must be swappable via configuration
- Control records are stored in **two separate tables** with distinct semantics:
  - **`received_record`** (`ReceivedRecord` entity) — written on the consumer thread immediately after deserialization; has a **unique constraint on `message_id`** which serves as the atomic duplicate guard
    - Fields: `messageId`, `interactionId`, `receivedAt`
  - **`published_record`** (`PublishedRecord` entity) — written after successful publish; no unique constraint
    - Fields: `messageId`, `interactionId`, `publishedAt`
- If an exception occurs at any stage (processing or publishing), the Control Component must **not** write a `PublishedRecord`; the failure is instead routed to the Dead Letter component
- Control Component must be a standalone, injectable service; it must not contain Kafka or processing logic directly

### Error Handling & Dead Letter
- Any exception raised during message processing or during Kafka output publishing is routed to a **Dead Letter Component**
- The Dead Letter Component receives and persists:
  - The original message payload as received from the input topic
  - A `reasonCode` (string/enum) describing the category of failure:
    - `DESERIALIZATION_ERROR` — message could not be parsed as JSON into the expected structure
    - `INVALID_MESSAGE_ID` — `body.messageId` is missing or not a valid UUID
    - `PROCESSING_ERROR` — an exception occurred during the simulated processing step
    - `PUBLISH_ERROR` — the processed message could not be published to the output topic
    - `CONTROL_RECORD_ERROR` — a control record (received/processed) could not be persisted
    - `DUPLICATE` — `messageId` was already processed (in-memory or DB gate)
    - `TIMEOUT` — the worker thread exceeded `processor-timeout-ms` before completing
  - A `messageId` where extractable
  - A `failedAt` timestamp
- Dead Letter storage implementation is pluggable and will be fully configured later; the interface must be defined now
- Failed messages must not block or stall the processing of subsequent messages

### Logging
- All log output must be written to **standard out** (stdout)
- Every log entry must include a **correlation ID** set to the `interactionId` from the message's `event` header
- The correlation ID must be bound to the thread context (e.g., MDC) at the point of message receipt and remain in scope for the full processing lifecycle of that message
- Log entries must be structured (e.g., JSON format) and include at minimum: timestamp, log level, correlation ID (`interactionId`), `messageId` (from body where available), and the log message
- Correlation ID must appear in all log entries related to processing, publishing, control record writes, and dead letter routing
- At a minimum, log at key lifecycle points: message received, processing started, publish succeeded, control record written, dead letter routed, and any exceptions

### Query APIs

#### General Rules
- All APIs are REST, returning JSON responses
- All timestamp parameters are ISO-8601 formatted strings (e.g., `2026-03-30T10:00:00Z`)
- `startTimestamp` defaults to **now minus 12 hours** when not provided
- `endTimestamp` is **optional**; when omitted, the query is open-ended (i.e., up to the most recent record)
- All endpoints must add `spring-boot-starter-web` to the application and be served on a configurable port

#### Control Log — Inbound Records
- `GET /api/control/inbound`
- Returns all `ReceivedRecord` entries within the specified time range
- Query parameters:
  - `startTimestamp` (optional, default: now minus 12 hours) — filters on `receivedAt >= startTimestamp`
  - `endTimestamp` (optional, no default) — filters on `receivedAt <= endTimestamp`
- Response fields per record: `messageId`, `interactionId`, `receivedAt`

#### Control Log — Outbound Records
- `GET /api/control/outbound`
- Returns all `PublishedRecord` entries within the specified time range
- Query parameters:
  - `startTimestamp` (optional, default: now minus 12 hours) — filters on `publishedAt >= startTimestamp`
  - `endTimestamp` (optional, no default) — filters on `publishedAt <= endTimestamp`
- Response fields per record: `messageId`, `interactionId`, `publishedAt`

#### Dead Letter Records
- `GET /api/deadletter`
- Returns all `DeadLetterRecord` entries within the specified time range
- Query parameters:
  - `startTimestamp` (optional, default: now minus 12 hours) — filters on `failedAt >= startTimestamp`
  - `endTimestamp` (optional, no default) — filters on `failedAt <= endTimestamp`
- Response fields per record: `messageId`, `interactionId`, `reasonCode`, `rawPayload`, `failedAt`

#### Configuration View
- `GET /api/config`
- Returns the current running configuration as JSON — useful for verifying live config in deployed instances
- Response fields: `kafka.bootstrapServers`, `kafka.consumerGroupId`, `kafka.consumerConcurrency`, `kafka.inputTopic`, `kafka.outputTopic`, `app.processorDelayMs`, `app.processorTimeoutMs`, `app.maxInFlight`, `app.workerThreads`, `app.siphonEnabledEvaluators`
- No query parameters

## Configuration
- Kafka bootstrap servers, input topic, output topic, `groupId`, and consumer concurrency must all be externally configurable under the `kafka.*` namespace (e.g., `application.yml` / environment variables)
- Application-specific settings that are not part of Kafka must be externalized under the `app.*` namespace to clearly distinguish them from Kafka/Spring internals:
  - `app.processing.processor-delay-ms` — sleep applied inside the worker thread before business logic (default: `20000`); set to `0` to disable
  - `app.processing.processor-timeout-ms` — hard timeout for the worker's execution (default: `10000`); exceeded workers are cancelled and dead-lettered with `TIMEOUT`
  - `app.processing.max-in-flight` — semaphore cap on concurrent in-flight messages; consumer thread blocks when limit is reached (default: `0` = unlimited)
  - `app.processing.worker-threads` — core pool size for the `ScheduledThreadPoolExecutor` dispatch threads (default: `32`); actual task execution uses virtual threads; a small value is sufficient
  - `app.siphon.enabled` — list of `SiphonEvaluator` event codes to activate (default: `[bde]`; empty = all active)
  - `kafka.topic.siphon-{event-code}` — one property per siphon evaluator (e.g. `kafka.topic.siphon-bde`)
- API server port must be externally configurable (`server.port`)
- No hardcoded Kafka or environment-specific values in source code

## Non-Functional
- Application must be stateless to support horizontal scaling across 10 instances
- Graceful shutdown must drain in-flight messages before stopping consumers

## Observability

### Metrics (Micrometer + Prometheus)
- The application must expose Micrometer metrics via Spring Boot Actuator at `/actuator/prometheus`
- Two timers must be recorded per message that enters the normal pipeline:
  - `kafka.processor.e2e.latency` (tagged by `eventType`) — started after the in-flight duplicate check, stopped in the worker's `finally` block; captures the full cycle including the scheduled delay
  - `kafka.processor.pipeline.latency` (tagged by `eventType`) — started at the top of the worker execution, stopped in the same `finally`; captures actual code execution time only
- Both timers must publish client-side percentiles: P50, P95, P99
- Four counters must be recorded:
  - `kafka.processor.messages.received` (tagged by `eventType`) — message entered the normal pipeline
  - `kafka.processor.messages.published` (tagged by `eventType`) — message successfully processed and acked
  - `kafka.processor.messages.siphoned` (tagged by `eventType`) — message fast-pathed via siphon
  - `kafka.processor.messages.failed` (tagged by `reason`) — message dead-lettered at any stage

### Health (Spring Boot Actuator)
- `GET /actuator/health` must return full component details (`show-details: always`)
- Built-in indicators must be active: `db` (datasource ping), `diskSpace`, `kafka` (broker connectivity)
- A custom `ProcessorHealthIndicator` (component name `processorThreadPool`) must report thread pool status:
  - `UP` — pool utilization < 100%
  - `OUT_OF_SERVICE` — pool saturated (active threads ≥ configured capacity)
  - `DOWN` — pool shut down or terminating
  - Details: `configuredThreads`, `activeThreads`, `poolSize`, `queuedTasks`, `completedTasks`, `utilizationPct`
- Exposed actuator endpoints: `health`, `info`, `prometheus`, `metrics`

## Developer Tooling

### Test Message Generator (`generateMessages` Gradle task)
- A Gradle task in `build.gradle` must generate synthetic Kafka message payloads for local testing
- Parameters (all optional, configurable via `-P` flags):
  - `count` — number of messages (default: 1000)
  - `pctNC`, `pctEND`, `pctTRM`, `pctRNW` — percentage distribution by event type; must sum to 100
  - `pctBDE` — percentage of END events that are backdated (default: 20)
  - `outDir` — output directory (default: `build/generated-messages`)
- Output: a single JSONL file (`messages-<count>.jsonl`) with one compact JSON object per line
- Windows shortcut: `gen-messages.cmd` (positional args: count, pctNC, pctEND, pctTRM, pctRNW, pctBDE)

### Local Kafka Stack (`docker-compose.yml`)
- A `docker-compose.yml` must provide a complete local test environment:
  - **Kafka** (KRaft mode, no Zookeeper) on port `9092`
  - **Kafka UI** on port `8081` (`http://localhost:8081`) — browse all topics, consumer group lag
  - **Prometheus** on port `9090` — scrapes `/actuator/prometheus` every 5 seconds
  - **Grafana** on port `3000` (`http://localhost:3000`, admin/admin) — auto-provisioned with Prometheus datasource
- `send-messages.cmd` — sends a generated JSONL file to the Kafka input topic via `kafka-console-producer` inside the Docker container

### Pipeline Monitor (`monitor-timings.ps1`)
- A PowerShell script must poll `/actuator/metrics` and report:
  - Message counts: received / published / siphoned / failed (with estimated in-flight)
  - E2E latency stats: avg, max, P50, P95, P99 from Micrometer timers
  - Pipeline latency stats: avg, max, P95 (worker execution only)
  - Failure breakdown by reason code
- `-Watch` flag for live refresh; `-Interval` to configure refresh rate; `-BaseUrl` to override target
