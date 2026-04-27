# Message Processing Concurrency Flow

Paste the diagram below into [mermaid.live](https://mermaid.live) to render it.

```mermaid
sequenceDiagram
    participant K as Kafka Broker
    participant CT as Consumer Thread
    participant DB as H2 Database
    participant SE as ScheduledExecutorService
    participant EP as IifMetricsEventProcessor
    participant OP as Output Topic

    Note over K,CT: Messages arrive continuously

    K->>CT: Message A (T=0)
    CT->>CT: Deserialize + in-memory duplicate check
    CT->>SE: execute(processA)
    CT-->>K: returns immediately

    K->>CT: Message B (T=1s)
    CT->>CT: Deserialize + in-memory duplicate check
    CT->>SE: execute(processB)
    CT-->>K: returns immediately

    K->>CT: Message C (T=2s)
    CT->>CT: Deserialize + in-memory duplicate check
    CT->>SE: execute(processC)
    CT-->>K: returns immediately

    Note over SE: A, B, C execute concurrently on worker threads

    SE->>DB: Write RECEIVED (A)
    SE->>EP: processor.process(A)
    Note over EP: SCD2 writes (raw, inclusion, pgPoints)<br/>publish enriched payload → output topic
    EP-->>SE: done
    SE->>DB: Write PUBLISHED (A)
    SE-->>K: Acknowledge offset A

    SE->>DB: Write RECEIVED (B)
    SE->>EP: processor.process(B)
    EP-->>SE: done
    SE->>DB: Write PUBLISHED (B)
    SE-->>K: Acknowledge offset B

    SE->>DB: Write RECEIVED (C)
    SE->>EP: processor.process(C)
    EP-->>SE: done
    SE->>DB: Write PUBLISHED (C)
    SE-->>K: Acknowledge offset C
```

## Key Points

- The **consumer thread makes zero DB calls** — fast operations only (deserialize, in-memory duplicate check, execute), then returns immediately
- Messages are dispatched immediately to the `ScheduledExecutorService` worker pool — no artificial delay
- **All messages are in-flight simultaneously**, each executing on their own worker thread
- `processor.process()` handles all business logic in one `@Transactional` boundary: SCD2 writes across three tables (raw, inclusion, pgPoints) and the Kafka publish — all commit or all roll back together
- `Write PUBLISHED` and acknowledgment only happen after `processor.process()` returns successfully
- **Acknowledgment happens on the worker thread** after the full pipeline completes — Kafka does not advance the offset until then
- If the app restarts mid-flight, un-acked messages are redelivered; the unique constraint on `ReceivedRecord.message_id` detects the duplicate INSERT and routes to dead letter safely
- If the app restarts mid-flight, un-acked messages are redelivered; the unique constraint on `ReceivedRecord.message_id` detects the duplicate INSERT and routes to dead letter safely
