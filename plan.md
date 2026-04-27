# Build Plan: Kafka Processor Application

## Overview

A Spring Boot 3.x / Java 21 application built with Gradle (Groovy DSL) that consumes JSON messages from a Kafka input topic, processes them concurrently, publishes results to a Kafka output topic, persists control records to an in-memory H2 database, and routes failures to a Dead Letter component. Deployed as 10 instances sharing a common Kafka `groupId`.

---

## Project Configuration

| Setting      | Value                        |
|--------------|------------------------------|
| Language     | Java 21                      |
| Framework    | Spring Boot 3.x (latest)     |
| Build Tool   | Gradle (Groovy DSL)          |
| Base Package | `com.example.kafkametrics` |
| Database     | H2 (in-memory, swappable)    |

---

## Phase 1 — Project Scaffold
*All steps parallel*

1. Initialize Gradle Groovy DSL project (`build.gradle`, `settings.gradle`)
2. Define dependencies in `build.gradle`:
   - `spring-boot-starter`
   - `spring-kafka`
   - `spring-boot-starter-data-jpa`
   - `com.h2database:h2`
   - `jackson-databind` (transitive via Spring)
   - `spring-boot-starter-web` (REST API)
   - `logstash-logback-encoder` (structured JSON logging)
3. Create `application.yml` with all externalized config placeholders:
   - `kafka.bootstrap-servers`, `kafka.consumer.group-id`
   - `kafka.topic.input`, `kafka.topic.output`
   - `kafka.consumer.concurrency` — set to `partitions ÷ instances` (e.g., 10 partitions / 10 instances = `1`; higher values waste idle threads)
   - `spring.datasource` (H2 in-memory)
   - `app.processing.processor-delay-ms` (default: `20000`) — sleep inside the worker thread before business logic; set to `0` to disable
   - `app.processing.processor-timeout-ms` (default: `10000`) — hard timeout for worker execution; exceeded messages are dead-lettered with `TIMEOUT`
   - `app.processing.max-in-flight` (default: `0`) — semaphore back-pressure; consumer blocks when limit is reached; `0` = unlimited
   - `app.processing.worker-threads` (default: `32`) — core pool size for the `ScheduledThreadPoolExecutor`; actual execution uses virtual threads; a small value (4–32) is sufficient
4. Create base package directory tree under `src/main/java/com/example/kafkametrics/`

**Relevant files:** `build.gradle`, `settings.gradle`, `src/main/resources/application.yml`

---

## Phase 2 — Message Model
*All steps parallel*

1. Create `EventHeader` record — fields: `interactionId` (String), `eventType` (String)
2. Create `MessageBody` record — field: `messageId` (String)
3. Create `KafkaMessage` envelope — fields: `event` (EventHeader), `body` (MessageBody)
4. Annotate all classes with Jackson `@JsonProperty`

**Package:** `com.example.kafkametrics.model`

**Relevant files:**
- `model/EventHeader.java`
- `model/MessageBody.java`
- `model/KafkaMessage.java`

---

## Phase 3 — Logging & MDC Configuration
*Depends on nothing; parallel with Phase 2*

1. Configure `logback-spring.xml` — structured JSON to stdout via `logstash-logback-encoder`;
   include fields: `timestamp`, `level`, `interactionId` (MDC), `messageId` (MDC), `message`
2. Create `MdcContext` utility — static helpers to set/clear `interactionId` and `messageId` in MDC
3. MDC must be set immediately after deserialization in the consumer and cleared in a `finally` block

**Package:** `com.example.kafkametrics.logging`

**Relevant files:**
- `src/main/resources/logback-spring.xml`
- `logging/MdcContext.java`

---

## Phase 4 — Control Component
*Depends on Phase 2*

1. Create **two** JPA entities in `com.example.kafkametrics.control`:
   - `ReceivedRecord` — `@Table(name = "received_record")` with `@UniqueConstraint` on `message_id`
     - Fields: `id` (auto), `messageId` (unique, not null), `interactionId`, `receivedAt` (Instant, not null)
   - `PublishedRecord` — `@Table(name = "published_record")`, no unique constraint
     - Fields: `id` (auto), `messageId`, `interactionId`, `publishedAt` (Instant)
2. Create `ReceivedRecordRepository extends JpaRepository<ReceivedRecord, Long>` with `existsByMessageId`, `findByReceivedAtGreaterThanEqual`, `findByReceivedAtBetween`
3. Create `PublishedRecordRepository extends JpaRepository<PublishedRecord, Long>` with `existsByMessageId`, `findByPublishedAtGreaterThanEqual`, `findByPublishedAtBetween`
4. Create `ControlService` interface — methods: `recordReceived(String messageId, String interactionId) throws DataIntegrityViolationException`, `recordPublished(String messageId, String interactionId)`
5. Create `ControlServiceImpl` implementing `ControlService` — `recordReceived` uses `saveAndFlush()` so the unique constraint fires within the same transaction
6. Configure `spring.datasource.url=jdbc:h2:mem:controldb` in `application.yml`

**Package:** `com.example.kafkametrics.control`

**Relevant files:**
- `control/ReceivedRecord.java`
- `control/PublishedRecord.java`
- `control/ReceivedRecordRepository.java`
- `control/PublishedRecordRepository.java`
- `control/ControlService.java`
- `control/ControlServiceImpl.java`

---

## Phase 5 — Dead Letter Component
*Depends on Phase 2; parallel with Phase 4*

1. Create `ReasonCode` enum: `DESERIALIZATION_ERROR`, `PROCESSING_ERROR`,
   `PUBLISH_ERROR`, `CONTROL_RECORD_ERROR`, `DUPLICATE`
2. Create `DeadLetterRecord` JPA entity:
   - `id` (auto), `messageId` (nullable), `interactionId` (nullable),
     `rawPayload` (String), `reasonCode` (ReasonCode), `failedAt` (Instant)
3. Create `DeadLetterRepository extends JpaRepository<DeadLetterRecord, Long>`
4. Create `DeadLetterService` interface —
   method: `handle(String rawPayload, ReasonCode reasonCode, String messageId, String interactionId)`
5. Create `DeadLetterServiceImpl` — persists to H2; implementation is pluggable (swappable later)

**Package:** `com.example.kafkametrics.deadletter`

**Relevant files:**
- `deadletter/ReasonCode.java`
- `deadletter/DeadLetterRecord.java`
- `deadletter/DeadLetterRepository.java`
- `deadletter/DeadLetterService.java`
- `deadletter/DeadLetterServiceImpl.java`

---

## Phase 6 — Kafka Producer
*Depends on Phase 2; parallel with Phases 4 & 5*

1. Create `KafkaProducerConfig` bean:
   - `StringSerializer` for key, `JsonSerializer` for value
   - Bootstrap servers from properties
   - Enable **idempotent producer**: `enable.idempotence=true`
   - Set **transactional ID**: `transactional-id` from properties (unique per instance, e.g. `kafkametrics-tx-${instance-id}`)
2. Create `KafkaProducerService` wrapping `KafkaTemplate<String, KafkaMessage>`:
   - `publish(KafkaMessage message)` — publishes to configured output topic within a Kafka transaction, awaits result,
     throws on failure; logs success at INFO with `interactionId` and `messageId`

**Package:** `com.example.kafkametrics.kafka`

**Relevant files:**
- `kafka/KafkaProducerConfig.java`
- `kafka/KafkaProducerService.java`

---

## Phase 7 — Kafka Consumer & Message Processor
*Depends on Phases 3, 4, 5, 6*

1. Create `KafkaConsumerConfig` bean:
   - `enable.auto.commit=false`, `AckMode.MANUAL_IMMEDIATE`
   - **`isolation.level=read_committed`** — EOS; only reads messages from committed transactions
   - **`auto.offset.reset=earliest`** — when a consumer group has no committed offset (first run, new group, or topic recreated), read from the beginning of the topic rather than skipping existing messages
   - `ErrorHandlingDeserializer` wrapping `JsonDeserializer` for value
   - `groupId` and `concurrency` from properties
   - Create a `ScheduledExecutorService` bean (`processingScheduler`) with `app.processing.worker-threads` threads
2. Create `MessageProcessorService.process(KafkaMessage)` — business logic stub (no delay here)
   - Throws `ProcessingException` on any failure
3. Create `KafkaConsumerListener` (`@KafkaListener`):
   - Receives raw `String` payload (preserves original for dead letter use)
   - Manually deserializes to `KafkaMessage` via `ObjectMapper`
   - Sets MDC (`interactionId`, `messageId`)
   - **In-flight duplicate gate**: `inFlightIds = ConcurrentHashMap.newKeySet()` field on the listener; `add()` returns false if already present — nanosecond check, no DB call on consumer thread. ID is removed in worker `finally` block.
   - **Restart/replay duplicate gate**: worker thread calls `controlService.recordReceived()` first; if `DataIntegrityViolationException` fires (in-memory set was lost on restart, row survived), route to dead letter + ack.
   - Consumer thread flow: deserialize → MDC → `inFlightIds.add()` (duplicate? dead letter + ack) → acquire semaphore (if `max-in-flight > 0`) → capture MDC snapshot → `processingScheduler.execute()` → return immediately
   - Worker thread flow: restore MDC → `recordReceived()` → process → publish → `recordPublished()` → ack → `inFlightIds.remove()` (in finally)

**Package:** `com.example.kafkametrics.kafka`

**Relevant files:**
- `kafka/KafkaConsumerConfig.java`
- `kafka/KafkaConsumerListener.java`
- `kafka/MessageProcessorService.java`

---

## Phase 8 — Query REST API
*Depends on Phases 4 & 5*

1. Add `spring-boot-starter-web` dependency (already in Phase 1)
2. Create `QueryController` (`@RestController`, base path `/api`) with three endpoints:

   **`GET /api/control/inbound`**
   - Query params: `startTimestamp` (ISO-8601, optional — defaults to now minus 12 hours), `endTimestamp` (ISO-8601, optional)
   - Returns all `ReceivedRecord` entries where `receivedAt >= startTimestamp` and, if provided, `receivedAt <= endTimestamp`
   - Response fields per record: `messageId`, `interactionId`, `receivedAt`

   **`GET /api/control/outbound`**
   - Query params: `startTimestamp` (optional, default now − 12h), `endTimestamp` (optional)
   - Returns all `PublishedRecord` entries where `publishedAt >= startTimestamp` and, if provided, `publishedAt <= endTimestamp`
   - Response fields per record: `messageId`, `interactionId`, `publishedAt`

   **`GET /api/deadletter`**
   - Query params: `startTimestamp` (optional, default now − 12h), `endTimestamp` (optional)
   - Returns all `DeadLetterRecord` entries where `failedAt >= startTimestamp` and, if provided, `failedAt <= endTimestamp`
   - Response fields per record: `messageId`, `interactionId`, `reasonCode`, `rawPayload`, `failedAt`

3. Create `TimeRangeHelper` utility — resolves `startTimestamp` to now minus 12 hours when null; passes `endTimestamp` through as-is (null = open-ended)
4. Add `findByReceivedAtBetween` / `findByReceivedAtGreaterThanEqual` and `existsByMessageId` query methods to `ReceivedRecordRepository`
5. Add `findByPublishedAtBetween` / `findByPublishedAtGreaterThanEqual` and `existsByMessageId` equivalents to `PublishedRecordRepository`
6. Add `findByFailedAtBetween` / `findByFailedAtGreaterThanEqual` query methods to `DeadLetterRepository`
7. Add `server.port` to `application.yml`

**Package:** `com.example.kafkametrics.api`

**Relevant files:**
- `api/QueryController.java`
- `api/TimeRangeHelper.java`

---

## Phase 9 — Application Entry Point & Wiring
*Depends on all prior phases*

1. Create `kafkametricsApplication` with `@SpringBootApplication`
2. Verify all Spring beans wire without circular dependencies
3. Confirm `application.yml` covers all config keys with sensible local-dev defaults

**Relevant files:**
- `kafkametricsApplication.java`
- `src/main/resources/application.yml`

---

## Phase 10 — Testing

1. *(parallel)* Unit test `ControlServiceImpl` — verify `ReceivedRecord` persisted with correct fields and `receivedAt`; verify second call with same `messageId` throws `DataIntegrityViolationException`; verify `PublishedRecord` persisted with correct fields
2. *(parallel)* Unit test `DeadLetterServiceImpl` — verify record persisted with correct
   `reasonCode`, `rawPayload`, timestamps
3. *(parallel)* Unit test `KafkaConsumerListener` with mocked dependencies:
   - Happy path: MDC set, `recordReceived` called (on worker), publish called, `recordPublished` called, ack sent
   - Deserialization failure → `DESERIALIZATION_ERROR`, no ack
   - `INVALID_MESSAGE_ID` — missing or malformed `body.messageId` → dead letter, ack, no output
   - Duplicate `messageId` in-flight — `listen()` called twice with same messageId while first is still scheduled (mock scheduler prevents execution) → `DUPLICATE`, ack and discard
   - Processing failure → `PROCESSING_ERROR`, no ack
   - Processing starts immediately (no consumer-side delay) — assert `processingScheduler.execute()` is used; assert worker calls `Thread.sleep(processorDelayMs)` before business logic when `processor-delay-ms > 0`
   - Publish failure → `PUBLISH_ERROR`, no ack
4. Integration tests with `@EmbeddedKafka` (topics: `test-input-topic`, `test-output-topic`, `test-siphon-bde-topic`):
   - Produce normal message to input topic → assert on output topic; `ReceivedRecord` + `PublishedRecord` exist in H2
   - Produce BDE message (`eventType=END`, `backdated=true`) → assert on siphon topic; nothing on output topic; no control records written
5. Verify structured log output contains `interactionId` and `messageId` fields
6. *(parallel)* Unit test `QueryController`:
   - `GET /api/control/inbound` with no params — assert `startTimestamp` defaults to now minus 12 hours, `endTimestamp` is open-ended
   - `GET /api/control/inbound` with both params — assert range is applied correctly
   - `GET /api/control/outbound` with and without params
   - `GET /api/deadletter` with and without params
   - Assert correct HTTP 200 responses and response body field mapping

---

## Phase 11 — UUID Validation & INVALID_MESSAGE_ID Reason Code

**Goal**: Reject messages whose `body.messageId` is missing or not a valid UUID before they enter the processing pipeline.

1. Add `INVALID_MESSAGE_ID` to the `ReasonCode` enum
2. In `KafkaConsumerListener.processDeferred()`, after deserialization, validate that `body.messageId` matches the UUID pattern (`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
3. On failure: call `DeadLetterService.handle(rawPayload, INVALID_MESSAGE_ID)`, log a warning, ack, return (no further processing)
4. Add unit test case to `KafkaConsumerListenerTest` covering invalid and missing `messageId`

---

## Phase 12 — Siphon Routing System

**Goal**: Replace single hard-coded BDE siphon check with an extensible, YAML-configurable evaluator chain.

1. Create `SiphonEvaluator` interface in `siphon` package:
   - `String eventCode()` — short identifier (e.g. `"bde"`)
   - `Optional<String> evaluate(KafkaMessage message)` — returns target topic or empty
2. Create `BdeSiphonEvaluator` (annotated `@Component`, event code `"bde"`):
   - Returns `Optional.of(kafka.topic.siphon-bde)` when `eventType == "END"` and `backdated == true`
3. Add `app.siphon.enabled` to `application.yml` (list of event codes, default `[bde]`)
4. Add `AppProperties` configuration-properties class (or expand existing) to bind `app.siphon.enabled`
5. Create `activeSiphonEvaluators` `@Bean` in a config class: filters the autowired `List<SiphonEvaluator>` by `app.siphon.enabled` (empty list = all active)
6. In `KafkaConsumerListener.listen()`, iterate `activeSiphonEvaluators`; first evaluator returning a non-empty Optional wins; publish to returned topic and return immediately
7. Add `kafka.topic.siphon-bde` to `application.yml`
8. Unit test `BdeSiphonEvaluator` — cover all four combinations of `eventType`/`backdated`; verify `eventCode()` returns `"bde"`
9. Update `KafkaConsumerListenerTest` siphon cases to exercise the new evaluator list injection path

---

## Phase 13 — Configuration View API

**Goal**: Expose a read-only JSON snapshot of current running configuration for operational visibility.

1. Ensure `AppProperties` (or equivalent `@ConfigurationProperties` class) binds:
   - `app.processing.processor-delay-ms`, `app.processing.processor-timeout-ms`, `app.processing.max-in-flight`, `app.processing.worker-threads`, `app.siphon.enabled`
2. Create `ConfigController` (`GET /api/config`):
   - Injects `AppProperties`, `@Value("${kafka.bootstrap-servers}")`, and other kafka props
   - Returns a flat JSON object with keys: `kafka.bootstrapServers`, `kafka.consumerGroupId`, `kafka.consumerConcurrency`, `kafka.inputTopic`, `kafka.outputTopic`, `app.processorDelayMs`, `app.processorTimeoutMs`, `app.maxInFlight`, `app.workerThreads`, `app.siphonEnabledEvaluators`
3. Unit test `ConfigControllerTest` — verify all expected fields are present using `MockMvc`

---

## Phase 14 — Micrometer Instrumentation

**Goal**: Instrument `KafkaConsumerListener` with Micrometer timers and counters; expose via Actuator Prometheus endpoint.

1. Add to `build.gradle`:
   ```groovy
   implementation 'org.springframework.boot:spring-boot-starter-actuator'
   implementation 'io.micrometer:micrometer-registry-prometheus'
   ```
2. Inject `MeterRegistry` into `KafkaConsumerListener` constructor
3. Record two `Timer.Sample`s per message:
   - **e2e**: start in `listen()` after in-flight duplicate check; stop in worker `finally` block; register as `kafka.processor.e2e.latency` tagged with `eventType`
   - **pipeline**: start at top of `processDeferred()` worker; stop in same `finally` block; register as `kafka.processor.pipeline.latency` tagged with `eventType`
4. Record four counters (increment at the appropriate point in the flow):
   - `kafka.processor.messages.received` — tag `eventType`
   - `kafka.processor.messages.published` — tag `eventType`
   - `kafka.processor.messages.siphoned` — tag `eventType`
   - `kafka.processor.messages.failed` — tag `reason` (ReasonCode enum name)
5. In `application.yml`, configure percentile histograms and percentiles (P50, P95, P99) for both timers; expose `health`, `info`, `prometheus`, `metrics` endpoints
6. Update `KafkaConsumerListenerTest` to pass `new SimpleMeterRegistry()` as last constructor argument

---

## Phase 15 — Health Monitoring (ProcessorHealthIndicator)

**Goal**: Expose thread pool utilization as a Spring Boot Actuator health component.

1. Create `ProcessorHealthIndicator` in `health` package (implements `HealthIndicator`):
   - Injects `ScheduledExecutorService processingScheduler` and `@Value("${app.processing.worker-threads:240}")`
   - Casts executor to `ThreadPoolExecutor` to read `activeCount`, `poolSize`, `queue.size()`, `completedTaskCount`
   - Status logic: `DOWN` if shut down/terminating; `OUT_OF_SERVICE` if `activeThreads >= configuredThreads`; otherwise `UP`
   - Details: `configuredThreads`, `activeThreads`, `poolSize`, `queuedTasks`, `completedTasks`, `utilizationPct`
2. In `application.yml`, set `management.endpoint.health.show-details: always`
3. Verify via `GET /actuator/health` that `processorThreadPool` component appears in the response

---

## Phase 16 — Developer Test Infrastructure

**Goal**: Make it easy to run the full stack locally and generate/send realistic test data.

### generateMessages Gradle Task
1. Add `generateMessages` task to `build.gradle`:
   - Configuration-time properties: `count`, `pctNC`, `pctEND`, `pctTRM`, `pctRNW`, `pctBDE`, `outDir` (all `project.findProperty()` calls **outside** `doLast`)
   - Outputs a single JSONL file: `<outDir>/messages-<count>.jsonl` (one compact JSON per line)
   - Each message: valid UUID `messageId`, random `interactionId`, timestamped, `eventType` sampled per configured distribution; END messages randomly marked `backdated=true` per `pctBDE`
2. Create `gen-messages.cmd` wrapper (passes positional args as Gradle `-P` flags)
3. Ensure Gradle task captures `project.findProperty()` at configuration time only — not inside `doLast` — to avoid the `Task.project at execution time` deprecation warning

### Docker Compose
4. Create `docker-compose.yml` with four services:
   - `kafka`: `apache/kafka:3.9.0`, KRaft mode, port `9092`
   - `kafka-ui`: `provectuslabs/kafka-ui:latest`, port `8081`, auto-connected to the `kafka` service
   - `prometheus`: `prom/prometheus:latest`, port `9090`, mounts `./prometheus.yml`; scrapes `host.docker.internal:8080/actuator/prometheus` every 5 s
   - `grafana`: `grafana/grafana:latest`, port `3000` (admin/admin), mounts `./grafana/provisioning` and auto-provisions Prometheus as default datasource
5. Create `prometheus.yml` scrape config
6. Create `grafana/provisioning/datasources/prometheus.yml`

### Send Messages Script
7. Create `send-messages.cmd`:
   - Accepts optional JSONL file path; defaults to latest `messages-*.jsonl` in `build/generated-messages`
   - Copies file into the `kafka` container, then pipes via `docker exec kafka kafka-console-producer`

### Monitor Script
8. Create `monitor-timings.ps1`:
   - Reads from `/actuator/metrics` REST API (no DB required)
   - Displays: message counts (received/published/siphoned/failed), e2e and pipeline latency (avg, max, P50, P95, P99), failure breakdown by reason code
   - `-Watch` flag, `-Interval` seconds, `-BaseUrl` override

---

## Component Interaction Flow

```
Kafka Input Topic
      │
      ▼
KafkaConsumerListener.listen()   (Spring Kafka listener thread)
  ├─ Deserialize JSON → KafkaMessage
  ├─ Set MDC (interactionId, messageId)
  ├─ [SiphonEvaluator chain] → first match? → KafkaProducerService.publish(siphonTopic) → ack, return
  ├─ In-flight duplicate check (ConcurrentHashMap) → duplicate? → DUPLICATE dead letter, ack, return
  ├─ Start e2e Timer.Sample
  ├─ Increment messages.received counter
  ├─ Acquire semaphore (if max-in-flight > 0)
  └─ Execute processDeferred() on ScheduledExecutorService (virtual thread)
           │
           ▼
KafkaConsumerListener.processDeferred()   (worker thread)
  ├─ Start pipeline Timer.Sample
  ├─ ControlService.recordReceived()
  ├─ MessageProcessorService.process()
  ├─ KafkaProducerService.publish(outputTopic)
  ├─ ControlService.recordPublished()
  ├─ Increment messages.published counter
  ├─ Acknowledgment.acknowledge()
  └─ [finally] stop both Timer.Samples; remove from in-flight map
       │
       └─ [ANY EXCEPTION] → DeadLetterService.handle(rawPayload, reasonCode)
                                    │          Increment messages.failed counter
                                    ▼
                               H2 (dead_letter_record)

ControlService        → H2 (control_record)
KafkaProducerService  → Kafka Output Topic / Siphon Topic
Actuator              → /actuator/health, /actuator/prometheus, /actuator/metrics
```

---

## Decisions & Scope

- **H2 is the initial persistence target** for control and dead letter records; both services are
  behind interfaces and fully swappable without touching consumer logic
- **Dead Letter does not re-publish to a Kafka DLQ** at this stage — deferred to later config
- **`MessageProcessorService.process()`** is a stub; business logic filled in separately
- **Offset is never committed on failure** — partition will replay on restart until DLQ skip logic is added
- **Concurrency** via a `ScheduledExecutorService` with virtual thread execution; `app.processing.worker-threads` controls the dispatch pool (platform threads, small value sufficient); Spring Kafka `concurrency` controls consumer threads; `app.processing.processor-delay-ms` is an optional per-worker sleep before business logic; `app.processing.max-in-flight` provides semaphore back-pressure on the consumer thread
- **Siphon routing** is first-match-wins across the `activeSiphonEvaluators` list; the list is filtered at startup by `app.siphon.enabled`; an empty enabled list activates all registered evaluators
- **Metrics** are recorded via Micrometer; no external metrics infrastructure required for the app itself — Prometheus and Grafana are provided by the Docker Compose stack for local use
