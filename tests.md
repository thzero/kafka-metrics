# Test Suite Documentation

## Overview

There are 5 test classes covering 15 individual tests. Tests are grouped into three categories:
- **Unit tests** — test a single class in isolation with mocked dependencies
- **Slice tests** — use a Spring-managed subset of the context (JPA layer or MVC layer only)
- **Integration tests** — spin up the full application context with an embedded Kafka broker

---

## ControlServiceImplTest

**Type:** JPA slice test (`@DataJpaTest` + real H2 database)  
**File:** `src/test/java/.../control/ControlServiceImplTest.java`

Tests the `ControlServiceImpl` class end-to-end against a real in-memory database. Verifies that the correct records are written to the correct tables with the correct field values.

| Test | What it does | Requirement covered |
|------|-------------|---------------------|
| `recordReceived_persistsRecordWithCorrectFields` | Calls `recordReceived("msg-1", "interaction-1")` and asserts that exactly one `ReceivedRecord` is saved with matching `messageId`, `interactionId`, and a non-null `receivedAt` timestamp. | Control Records — a record is written upon receiving a message capturing `messageId`, `interactionId`, and `receivedAt`. |
| `recordReceived_duplicateMessageId_throwsDataIntegrityViolation` | Calls `recordReceived` twice with the same `messageId` and asserts the second call throws `DataIntegrityViolationException`. | Duplicate detection — the unique constraint on `received_record.message_id` is the atomic duplicate guard; no separate SELECT is needed. |
| `recordPublished_persistsRecordWithCorrectFields` | Calls `recordPublished("msg-2", "interaction-2")` and asserts that exactly one `PublishedRecord` is saved with matching fields and a non-null `publishedAt` timestamp. | Control Records — a separate record is written upon successful publish capturing `messageId`, `interactionId`, and `publishedAt`. |
| `findByReceivedAtGreaterThanEqual_returnsRecordsInRange` | Saves a `ReceivedRecord`, then queries `receivedRepository.findByReceivedAtGreaterThanEqual(before)` and asserts the record is returned. | Query APIs — `GET /api/control/inbound` filters on `receivedAt >= startTimestamp`. |

---

## DeadLetterServiceImplTest

**Type:** JPA slice test (`@DataJpaTest` + real H2 database)  
**File:** `src/test/java/.../deadletter/DeadLetterServiceImplTest.java`

Tests the `DeadLetterServiceImpl` class against a real in-memory database. Verifies that dead letter records are persisted with all required fields.

| Test | What it does | Requirement covered |
|------|-------------|---------------------|
| `handle_persistsRecordWithCorrectFields` | Calls `handle(payload, PROCESSING_ERROR, "msg-1", "interaction-1")` and asserts all fields (`rawPayload`, `reasonCode`, `messageId`, `interactionId`, `failedAt`) are saved correctly. | Dead Letter — the component persists the original payload, a `reasonCode`, `messageId`, and a `failedAt` timestamp. |
| `handle_allowsNullMessageIdAndInteractionId` | Calls `handle` with `null` for both `messageId` and `interactionId` (simulating a deserialization failure where no IDs are extractable) and asserts the record is still saved. | Dead Letter — `messageId` and `interactionId` are nullable to handle deserialization failures where no IDs can be extracted. |
| `findByFailedAtGreaterThanEqual_returnsRecordsInRange` | Saves a dead letter record, then queries `findByFailedAtGreaterThanEqual(before)` and asserts it is returned. | Query APIs — `GET /api/deadletter` filters on `failedAt >= startTimestamp`. |

---

## KafkaConsumerListenerTest

**Type:** Pure unit test (`@ExtendWith(MockitoExtension.class)`)  
**File:** `src/test/java/.../kafka/KafkaConsumerListenerTest.java`

Tests the full message processing pipeline in `KafkaConsumerListener` with all dependencies mocked. Uses a real single-threaded `ScheduledExecutorService` with a zero delay (set via `ReflectionTestUtils`) so scheduled work executes synchronously within the test. Each test calls `awaitScheduler()` to ensure all deferred work has completed before assertions run.

| Test | What it does | Requirement covered |
|------|-------------|---------------------|
| `happyPath_writesControlRecords_publishesAndAcks` | Sends a valid message and asserts the full lifecycle executes in order: `recordReceived` → `process` → `publish` → `recordPublished` → `acknowledge`. Also asserts `deadLetterService` is never called. | Full message processing pipeline — offset is committed only after successful publish and control record write; no dead letter on success. |
| `deserializationFailure_routesToDeadLetter_noAck` | Sends malformed JSON and asserts `deadLetterService.handle` is called with `DESERIALIZATION_ERROR` and null IDs, and that `acknowledge` is never called. | Error handling — deserialization failures route to dead letter with `DESERIALIZATION_ERROR`; no ack so the partition does not advance. |
| `bdeEvent_siphonsToSiphonTopic_acks` | Sends a message with `eventType: END` and `backdated: true` and asserts `kafkaProducerService.siphon()` is called, `acknowledge` is called, and `controlService`/`process`/`publish`/`deadLetterService` are never invoked. | Backdated Endorsement siphon — `END + backdated=true` bypasses the entire processing pipeline and is forwarded directly to the siphon topic on the consumer thread. |
| `bdeEvent_siphonFailure_noAck` | Configures `kafkaProducerService.siphon()` to throw `KafkaPublishException` and asserts `acknowledge` is never called and no other services are invoked. | Backdated Endorsement siphon failure — a failed siphon publish returns without ack so the message is redelivered. |
| `duplicateMessage_inFlight_routesToDeadLetter_acks` | Creates a separate listener backed by a mock scheduler (so the first message's worker task is captured but never executed). Calls `listen` twice with the same payload; asserts the second call is routed to dead letter with `DUPLICATE`, its ack is called, and `controlService`/`process`/`publish` are never invoked. | Duplicate detection — `ConcurrentHashMap.newKeySet().add()` returns `false` on the second call because the messageId is still held in the in-flight set; the duplicate is rejected on the consumer thread with zero DB cost. |
| `processingFailure_routesToDeadLetter_noAck` | Configures `messageProcessorService.process` to throw `ProcessingException` and asserts dead letter is called with `PROCESSING_ERROR` and `acknowledge` is never called. | Error handling — processing failures route to dead letter with `PROCESSING_ERROR`; no ack. |
| `publishFailure_routesToDeadLetter_noAck` | Configures `kafkaProducerService.publish` to throw `KafkaPublishException` and asserts dead letter is called with `PUBLISH_ERROR`, `acknowledge` is never called, and `recordPublished` is never called. | Error handling — publish failures route to dead letter with `PUBLISH_ERROR`; no ack; the PUBLISHED control record must not be written on failure. |

---

## QueryControllerTest

**Type:** MVC slice test (`@WebMvcTest`)  
**File:** `src/test/java/.../api/QueryControllerTest.java`

Tests the REST endpoints in `QueryController` using `MockMvc`. All repositories are mocked; no database is involved. Verifies that the correct repository methods are called and that responses are correctly serialized to JSON.

| Test | What it does | Requirement covered |
|------|-------------|---------------------|
| `getInbound_noParams_defaultsToLast12Hours` | Calls `GET /api/control/inbound` with no parameters and asserts `findByReceivedAtGreaterThanEqual` is called (open-ended range from 12 hours ago). | Query API — `startTimestamp` defaults to now minus 12 hours when not provided; `endTimestamp` is optional and open-ended when omitted. |
| `getInbound_withBothParams_appliesRange` | Calls `GET /api/control/inbound?startTimestamp=…&endTimestamp=…` and asserts `findByReceivedAtBetween` is called and the response body contains the expected `messageId`. | Query API — when both parameters are supplied, the query applies a closed date range; response includes `messageId` and `interactionId`. |
| `getOutbound_noParams_defaultsToLast12Hours` | Calls `GET /api/control/outbound` with no parameters and asserts `findByPublishedAtGreaterThanEqual` is called on the published repository. | Query API — `GET /api/control/outbound` uses `publishedAt` for filtering and defaults to 12-hour lookback. |
| `getDeadLetter_noParams_defaultsToLast12Hours` | Calls `GET /api/deadletter` with no parameters and asserts `findByFailedAtGreaterThanEqual` is called. | Query API — `GET /api/deadletter` defaults `startTimestamp` to now minus 12 hours. |
| `getDeadLetter_withBothParams_appliesRange` | Calls `GET /api/deadletter?startTimestamp=…&endTimestamp=…` and asserts the response body contains `messageId` and `reasonCode`. | Query API — dead letter response includes `messageId`, `interactionId`, `reasonCode`, `rawPayload`, and `failedAt`. |

---

## KafkaIntegrationTest

**Type:** Full integration test (`@SpringBootTest` + `@EmbeddedKafka`)  
**File:** `src/test/java/.../integration/KafkaIntegrationTest.java`

Boots the complete Spring application context with an embedded single-partition Kafka broker. Publishes a real message to the input topic and waits for it to appear on the output topic. Uses `isolation.level=read_committed` on the test consumer to verify the transactional producer commits its transaction. Processing delay is overridden to 0 ms for test speed.

| Test | What it does | Requirement covered |
|------|-------------|---------------------|
| `messageFlowProducesOutputAndWritesControlRecords` | Publishes a message to the input topic, waits up to 10 seconds for it to appear on the output topic, then asserts the `messageId` matches. Also asserts that both a `ReceivedRecord` and a `PublishedRecord` exist in H2 for the message. | End-to-end flow — confirms Kafka EOS (transactional producer, `read_committed` isolation), message processing, output publish, and control record persistence all work together as a system. |

---

## Requirements Coverage Matrix

| Requirement | Tests |
|-------------|-------|
| Deserialization of JSON envelope into typed objects | `happyPath_writesControlRecords_publishesAndAcks`, `messageFlowProducesOutputAndWritesControlRecords` |
| No duplicate processing — in-memory gate + DB unique constraint | `recordReceived_duplicateMessageId_throwsDataIntegrityViolation`, `duplicateMessage_inFlight_routesToDeadLetter_acks` |
| Deferred processing via `ScheduledExecutorService` | All `KafkaConsumerListenerTest` tests (zero-delay scheduler proves the scheduling path executes) |
| `ReceivedRecord` written on worker thread before business logic | `happyPath_writesControlRecords_publishesAndAcks` (verifies `recordReceived` is called after `awaitScheduler`) |
| `PublishedRecord` written only on full success | `publishFailure_routesToDeadLetter_noAck` (asserts `recordPublished` is NOT called on failure) |
| Manual ack only on full success | All `KafkaConsumerListenerTest` failure tests assert `acknowledge` is never called |
| Dead letter with correct `reasonCode` | `deserializationFailure_…`, `duplicateMessage_…`, `processingFailure_…`, `publishFailure_…`, `handle_persistsRecordWithCorrectFields` |
| Dead letter accepts null IDs (deserialization failure) | `handle_allowsNullMessageIdAndInteractionId` |
| Kafka EOS (transactional producer, `read_committed`) | `messageFlowProducesOutputAndWritesControlRecords` |
| `GET /api/control/inbound` — default and range filtering | `getInbound_noParams_defaultsToLast12Hours`, `getInbound_withBothParams_appliesRange` |
| `GET /api/control/outbound` — default filtering | `getOutbound_noParams_defaultsToLast12Hours` |
| `GET /api/deadletter` — default and range filtering | `getDeadLetter_noParams_defaultsToLast12Hours`, `getDeadLetter_withBothParams_appliesRange` |
| Full end-to-end pipeline | `messageFlowProducesOutputAndWritesControlRecords` |
