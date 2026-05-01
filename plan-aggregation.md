# Metric Aggregation — Build Plan

## Overview

Daily fan-out pattern: a ShedLock-protected job refreshes a materialized view then publishes
batches of agency numbers to a Kafka work topic. All instances consume the work topic and
upsert pre-aggregated results into a result table.

## Data Flow

```
Two triggers (both ShedLock-protected, same lock name):
  ┌─ @Scheduled cron
  └─ POST /api/jobs/daily-aggregate?jobDate=2026-05-01
        │
        ├─ 1. Acquire ShedLock "dailyAggregateJob"
        │      (second caller → 409 Conflict)
        │
        ├─ 2. REFRESH MATERIALIZED VIEW CONCURRENTLY mv_points_by_product_agency
        │      (fails → release lock, throw, no fan-out)
        │
        ├─ 3. SELECT DISTINCT agency_nbr FROM mv_points_by_product_agency
        │
        ├─ 4. Partition into batches of N (configurable, default 50)
        │
        └─ 5. Publish one Kafka message per batch
               topic:   iif.agency-aggregate-work
               key:     batch index ("0", "1", "2"...)
               payload: { "jobDate": "2026-05-01", "agencyNumbers": ["A1","A2",...] }

All instances (same consumer group, topic: iif.agency-aggregate-work)
        │
        ├─ 6. SELECT agency_nbr, SUM(total_pg_points)
        │      FROM mv_points_by_product_agency
        │      WHERE agency_nbr IN (...)
        │      GROUP BY agency_nbr
        │
        └─ 7. Upsert → iif_agency_daily_aggregate
               ON CONFLICT (agency_nbr, job_date) DO UPDATE
               (idempotent — safe on redelivery)
```

## Kafka Message

**Topic:** `iif.agency-aggregate-work`
**Partitions:** match expected max instance count
**Retention:** short (hours) — transient work items

```json
{
  "jobDate": "2026-05-01",
  "agencyNumbers": ["AGT001", "AGT002", "AGT003"]
}
```

```java
public record AgencyAggregateBatch(LocalDate jobDate, List<String> agencyNumbers) {}
```

## Components

| Component | Responsibility |
|---|---|
| `DailyAggregateJob` | `run(LocalDate)` — refresh MV, query agencies, publish batches |
| `JobController` | `POST /api/jobs/daily-aggregate` — manual trigger via `LockingTaskExecutor` |
| `AgencyBatchPublisher` | Partitions agency list into batches, publishes to work topic |
| `AgencyAggregateWorkConsumer` | Kafka listener — receives batch, queries MV, upserts results |
| `AgencyAggregateRepository` | MV `IN` query + upsert to `iif_agency_daily_aggregate` |
| `MvRefreshRepository` | `REFRESH MATERIALIZED VIEW CONCURRENTLY` |
| `mv_points_by_product_agency` | MV — joins iif_metric_points + iif_metrics_raw + iif_metric_inclusion + policy_aor |
| `iif_agency_daily_aggregate` | Result table — one row per `(agency_nbr, job_date)` |

## API

```
POST /api/jobs/daily-aggregate
     ?jobDate=2026-05-01   (optional, defaults to today)

202 Accepted  — job started
409 Conflict  — already running
```

## Configuration

```yaml
app:
  jobs:
    daily-aggregate:
      cron: "0 0 2 * * *"
      batch-size: 50
      lock-at-most: PT4H
```

---

## Build Phases

### Phase 1 — Database
1. **V3 migration** — materialized view `mv_points_by_product_agency` joining
   `iif_metric_points` + `iif_metrics_raw` + `iif_metric_inclusion` + `policy_aor`,
   unique index for `CONCURRENTLY` refresh
2. **V4 migration** — `iif_agency_daily_aggregate` result table + ShedLock table

### Phase 2 — Repository layer
4. `MvRefreshRepository` — `REFRESH MATERIALIZED VIEW CONCURRENTLY`
5. `AgencyAggregateRepository` — `IN` query against MV + upsert to result table
6. Drop the now-redundant `@Query` on `IIifMetricsPgPointsRepository` (MV replaces it)

### Phase 3 — Kafka
7. `AgencyAggregateBatch` record
8. New Kafka topic config — `iif.agency-aggregate-work` (producer + consumer beans)
9. `AgencyBatchPublisher` — partitions agency list, publishes
10. `AgencyAggregateWorkConsumer` — listener, calls repository, upserts results

### Phase 4 — Job orchestration
11. ShedLock dependency in `build.gradle`
12. `DailyAggregateJob` — `run(LocalDate)`: refresh → query agencies → publish
13. `@Scheduled` + `@SchedulerLock` wrapper

### Phase 5 — API
14. `JobController` — `POST /api/jobs/daily-aggregate` using `LockingTaskExecutor`
15. Bruno request for manual trigger

### Phase 6 — Config + tests
16. `application.yml` additions — topic name, batch size, cron, lock-at-most
17. Unit tests — `DailyAggregateJob`, `AgencyBatchPublisher`, `AgencyAggregateWorkConsumer`

---

## Key Design Properties

- **Idempotent** — upsert on `(agency_nbr, job_date)`; redelivered messages are safe
- **Re-runnable** — API trigger with explicit `jobDate` reruns any past date
- **No barrier needed** — MV refreshed before any messages published; workers read consistent snapshot
- **Horizontally scalable** — add instances, Kafka rebalances partitions automatically
- **ShedLock on both paths** — scheduler and API share same lock name; concurrent triggers safely rejected
- **NULL-safe joins** — all `asset_id` joins use `IS NOT DISTINCT FROM`
