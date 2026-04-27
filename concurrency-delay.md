# Kafka Concurrency and the Processing Delay

## How Kafka partitions, instances, and concurrency relate

A Kafka topic is divided into partitions. Each partition is assigned to exactly one consumer in a consumer group at a time. In this deployment model:

- **10 partitions, 10 instances, `concurrency: 1`** → each instance owns 1 partition
- Each instance has one consumer thread polling its assigned partition
- Messages within a partition are delivered **one at a time** — `listen()` must return before the next message is delivered from that partition

`concurrency` controls how many consumer threads exist **per instance**. Setting it to 1 means one thread handles all partitions assigned to that instance. Setting it higher with a 1-partition-per-instance deployment is wasteful — the extra threads have nothing to consume.

---

## The problem: a 20-second delay requirement

The business requirement is: **defer processing by 20 seconds** after a message is received (upstream race condition buffer). There are two ways to implement this.

### Option 1: `Thread.sleep` on the consumer thread

```
Instance (owns partition 3):
  recv A → sleep(20s) → process A → recv B → sleep(20s) → process B → ...
  Throughput: 1 message per 20+ seconds per instance
```

At 10 msg/sec arriving across all partitions, each instance receives 1 msg/sec. One message per 20 seconds means consumer lag grows by ~1 message per second per partition, unboundedly. You'd need to either:

- Scale instances to match `arrival_rate × delay_seconds` (at 1 msg/sec per partition × 20s = 20 partitions/instances just to keep up with a single partition's lag)
- Or accept unbounded consumer lag

### Option 2: `ScheduledExecutorService.schedule(task, 20s)` — what this code does

```
Instance (owns partition 3):
  recv A → schedule(A, 20s) → return immediately
  recv B → schedule(B, 20s) → return immediately
  recv C → schedule(C, 20s) → return immediately
  ...
  T+20s: A fires on worker thread, B fires, C fires — all in parallel
  Throughput: limited only by how fast the consumer can poll
```

The consumer thread returns in **milliseconds**. All in-flight messages count down their 20 seconds simultaneously in the scheduler's delay queue — **consuming zero threads while waiting**. When the delay expires, a worker thread executes the actual processing.

---

## Why the scheduler costs almost nothing while waiting

`ScheduledExecutorService` internally uses a priority queue ordered by fire time. Submitting `schedule(task, 20s)` puts the task in that queue and returns immediately. No thread is allocated until the delay fires. 10,000 messages all waiting their 20 seconds occupy 10,000 queue entries — not 10,000 threads.

The `worker-threads: 200` pool size controls how many tasks can **execute** simultaneously, not how many can be **waiting**. The number of concurrently executing tasks at any moment is bounded by `arrival_rate × execution_time_seconds` (typically well under 200).

---

## Summary

| Approach | Throughput | Threads consumed during wait | Scales with |
|---|---|---|---|
| `Thread.sleep` on consumer thread | 1 msg / 20s per instance | 1 (the consumer thread is blocked) | More instances + partitions |
| `ScheduledExecutorService.schedule` | poll rate (milliseconds per msg) | 0 (delay queue only) | Nothing — it's essentially free |

The scheduler is the correct mechanism precisely because it decouples "I received a message" from "I need a thread to wait 20 seconds" — the delay queue handles the timing, and threads are only consumed when there is actual work to do.
