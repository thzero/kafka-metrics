# Message Processing Concurrency Flow

Paste the diagram below into [mermaid.live](https://mermaid.live) to render it.

```mermaid
sequenceDiagram
    participant K as Kafka Broker
    participant CT as Consumer Thread
    participant DB as H2 Database
    participant SV as SiphonEvaluators
    participant SE as ScheduledExecutorService
    participant MP as MessageProcessorService
    participant EP as EventProcessor
    participant KP as KafkaProducerService
    participant OP as Output / Siphon Topics

    Note over K,CT: Messages arrive continuously

    K->>CT: Message A — siphon match (T=0)
    CT->>CT: Deserialize
    CT->>SV: Evaluate siphon rules
    SV-->>CT: BdeSiphonEvaluator matches → siphon-bde-topic
    CT->>KP: publish(messageId, rawPayload, siphon-bde-topic)
    KP->>OP: Publish A (siphon fast-path)
    CT-->>K: Acknowledge offset A immediately

    K->>CT: Message B — no siphon match (T=1s)
    CT->>CT: Deserialize + in-memory duplicate check
    CT->>SV: Evaluate siphon rules
    SV-->>CT: No match
    CT->>SE: schedule(processB, delay=20s)
    CT-->>K: returns immediately

    K->>CT: Message C — no siphon match (T=2s)
    CT->>CT: Deserialize + in-memory duplicate check
    CT->>SV: Evaluate siphon rules
    SV-->>CT: No match
    CT->>SE: schedule(processC, delay=20s)
    CT-->>K: returns immediately

    Note over SE: B, C waiting simultaneously on separate worker threads

    SE->>SE: Message B fires (T=20s) — restore MDC
    SE->>DB: Write RECEIVED (B)
    SE->>MP: process(message, rawPayload)
    MP->>EP: dispatch by eventType → e.g. DefaultEventProcessor
    EP->>KP: publish(messageId, outputJson, output-topic)
    KP->>OP: Publish B (output)
    SE->>DB: Write PUBLISHED (B)
    SE-->>K: Acknowledge offset B

    SE->>SE: Message C fires (T=22s) — restore MDC
    SE->>DB: Write RECEIVED (C)
    SE->>MP: process(message, rawPayload)
    MP->>EP: dispatch by eventType → e.g. DefaultEventProcessor
    EP->>KP: publish(messageId, outputJson, output-topic)
    KP->>OP: Publish C (output)
    SE->>DB: Write PUBLISHED (C)
    SE-->>K: Acknowledge offset C
```

## Key Points

- The **consumer thread makes zero DB calls** — fast operations only (deserialize, in-memory duplicate check, siphon check / schedule), then returns immediately
- **Siphon fast-path**: if any `SiphonEvaluator` matches, the raw payload is published directly to the siphon topic on the consumer thread — no scheduling delay
- **Normal path**: messages are scheduled on a `ScheduledExecutorService` worker thread with a configurable delay (default 20 s)
- When a worker fires, `MessageProcessorService` routes by `eventType` to the matching `EventProcessor` implementation (falls back to `DefaultEventProcessor` for unknown types); the processor serializes the message and calls `KafkaProducerService.publish()`
- **All messages are in-flight simultaneously**, each with their own independent countdown on a separate worker thread
- The **worker thread pool** is sized to the maximum expected in-flight messages: `msg/sec × delay-ms / 1000` (e.g., 12 msg/sec × 20 s = 240 threads)
- **Acknowledgment happens on the worker thread** after the full pipeline completes — Kafka does not advance the offset until then
- If the app restarts mid-flight, un-acked messages are redelivered; the unique constraint on `ReceivedRecord.message_id` detects the duplicate INSERT and routes to dead letter safely
