# Metric Aggregation — Build Plan

## Architecture Decision — Fan-Out vs Single Query

Two approaches were evaluated for writing aggregated results:

**Option A — Single `INSERT INTO ... SELECT GROUP BY` (no Kafka):**
- One sequential scan of the MV, one hash aggregate, one bulk upsert
- PostgreSQL uses internal parallel workers (`max_parallel_workers_per_gather`) — completes in seconds
- No Kafka overhead; simplest possible implementation
- Downside: whole job must re-run on failure; no retry granularity per agency

**Option B — Kafka fan-out with batches of agencies per message:**
- 100k agencies → ~100 messages (configurable batch size ~1000)
- 10 app instances consume in parallel; each fires one `GROUP BY agency_nbr IN (...)` query
- Retry granularity: one failed batch retries, not the whole dataset
- Decouples MV refresh from write path
- Actual throughput vs Option A is comparable — PostgreSQL's internal parallelism is already efficient

**Decision: keep Kafka fan-out (Option B).** Not because it is measurably faster than Option A — it likely is not — but because of retry granularity, decoupling, and the ability to scale horizontally across 10 instances. One-message-per-agency (100k messages) was explicitly ruled out.

---

## Oracle Exadata Portability Assessment

If this system were migrated to Oracle Exadata instead of PostgreSQL, the following changes would be required:

**SQL syntax changes:**
- `IS NOT DISTINCT FROM` → `(col1 = col2 OR (col1 IS NULL AND col2 IS NULL))`
- `ON CONFLICT DO UPDATE` (upsert) → `MERGE INTO ... USING ... ON ... WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`
- `BIGSERIAL` → `NUMBER GENERATED ALWAYS AS IDENTITY`
- `TIMESTAMPTZ` → `TIMESTAMP WITH TIME ZONE`
- `BOOLEAN` → `NUMBER(1)` or `CHAR(1)` — Oracle has no native boolean until 23c
- `VARCHAR(255)` → `VARCHAR2(255)`
- Partial indexes (`WHERE eff_end_dt = ...`) → not supported; replace with function-based indexes
- `NULLS NOT DISTINCT` on unique indexes → not supported; use function-based unique index with `NVL(asset_id, '##NULL##')`

**Materialized view changes:**
- `REFRESH MATERIALIZED VIEW CONCURRENTLY` → `DBMS_MVIEW.REFRESH('mv_name', 'C')` (complete) or `'F'` (fast/incremental)
- Fast refresh requires `MATERIALIZED VIEW LOG ON` each base table — more setup but potentially much faster than full refresh
- Complete refresh locks the MV during refresh (no CONCURRENTLY equivalent); fast refresh does not

**Exadata-specific advantages:**
- Smart Scan offloads full table scans and aggregations to storage cells — the MV scan + GROUP BY runs on the storage layer, not DB CPU; dramatically faster on large datasets
- Storage indexes (automatic) on `agency_nbr`, `excluded_ind` — eliminate I/O for filtered queries without explicit index creation
- Hybrid Columnar Compression (HCC) on the MV — 10–50x compression, proportionally reduces scan I/O
- In-Memory Column Store: MV loaded into IMCS, aggregates run at memory bandwidth speed

**Architecture impact:**
- The fan-out vs single-query decision shifts further toward Option A on Exadata — Smart Scan makes the single full-scan `GROUP BY` even faster relative to many parallel index-range scans
- The MV itself becomes less necessary as a performance layer; querying base tables directly with Smart Scan may be sufficient
- The Kafka fan-out still provides retry granularity and decoupling regardless of DB platform

**Java/build changes:**
- Swap `org.postgresql:postgresql` JDBC driver for `com.oracle.database.jdbc:ojdbc11`
- Flyway migration files require full DDL syntax rewrite; migration logic and structure are unchanged

---

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
        └─ 4. Publish one Kafka message per agency
               topic:   iif.agency-aggregate-work
               key:     agency_nbr
               payload: { "jobDate": "2026-05-01", "agencyNbr": "AGT001" }

All instances (same consumer group, topic: iif.agency-aggregate-work)
        │
        ├─ 5a. SELECT excluded_ind, COUNT(*) AS total_items
        │       FROM mv_points_by_product_agency
        │       WHERE agency_nbr = :agencyNbr
        │       GROUP BY excluded_ind
        │
        ├─ 5b. SELECT asset_product_ent_cd, SUM(pg_points_value) AS total_pg_points
        │       FROM mv_points_by_product_agency
        │       WHERE agency_nbr = :agencyNbr AND excluded_ind = false
        │       GROUP BY asset_product_ent_cd
        │
        └─ 6. Upsert results → iif_agency_daily_aggregate
               ON CONFLICT (agency_nbr, job_date, aggregate_type) DO UPDATE
               (idempotent — safe on redelivery)
```

## Kafka Message

**Topic:** `iif.agency-aggregate-work`
**Partitions:** match expected max instance count
**Retention:** short (hours) — transient work items

```json
{
  "jobDate": "2026-05-01",
  "agencyNbr": "AGT001"
}
```

```java
public record AgencyAggregateWork(LocalDate jobDate, String agencyNbr) {}
```

## Components

| Component | Responsibility |
|---|---|
| `DailyAggregateJob` | `run(LocalDate)` — refresh MV, query distinct agencies, publish one message per agency |
| `JobController` | `POST /api/jobs/daily-aggregate` — manual trigger via `LockingTaskExecutor` |
| `AgencyWorkPublisher` | Publishes one message per agency to work topic |
| `AgencyAggregateWorkConsumer` | Kafka listener — receives one agency, runs COUNT + SUM queries, upserts results |
| `AgencyAggregateRepository` | Per-agency COUNT/SUM queries against MV + upsert to `iif_agency_daily_aggregate` |
| `MvRefreshRepository` | `REFRESH MATERIALIZED VIEW CONCURRENTLY` |
| `YearEndSnapshotJob` | Runs Jan 1 — deletes prior year rows, inserts from MV into snapshot table |
| `mv_points_by_product_agency` | MV — item-level join of active rows; one row per `(agreement_product_nbr, asset_id)` |
| `iif_agency_daily_aggregate` | Result table — one row per `(agency_nbr, job_date, aggregate_type)` |
| `iif_agency_yearend_snapshot` | Partitioned snapshot table — item-level, one row per `(production_year, agreement_product_nbr, asset_id)` |

### MV: `mv_points_by_product_agency`

Joins four tables, active rows only (`eff_end_dt = HIGH_DATE`). No aggregation — one row per `(agreement_product_nbr, asset_id)`. Callers aggregate and filter `excluded_ind` as needed.

| Column | Source | Notes |
|---|---|---|
| `agreement_product_nbr` | `iif_metric_points.agreement_product_nbr` | join key; unique with `asset_id` |
| `asset_id` | `iif_metric_points.asset_id` | nullable join key |
| `agency_nbr` | `policy_aor.agency_nbr` | joined on `agreement_product_nbr` |
| `asset_product_ent_cd` | `iif_metrics_raw.asset_product_ent_cd` | joined on `(agreement_product_nbr, asset_id)` |
| `excluded_ind` | `iif_metric_inclusion.excluded_ind` | filter dimension — not pre-aggregated |
| `pg_points_value` | `iif_metric_points.pg_points_value` | raw value; callers aggregate |

Filters:
- `iif_metric_points.eff_end_dt = HIGH_DATE` — active points rows only
- `iif_metrics_raw.eff_end_dt = HIGH_DATE` — active raw rows only
- `iif_metric_inclusion.eff_end_dt = HIGH_DATE` — active inclusion rows only
- `asset_id` joins use `IS NOT DISTINCT FROM` — NULL-safe

Unique index (required for `CONCURRENTLY`):
```sql
CREATE UNIQUE INDEX ON mv_points_by_product_agency (agreement_product_nbr, asset_id) NULLS NOT DISTINCT;
```

## Aggregates

### Aggregate 1 — Current: count by agency + inclusion status
```sql
SELECT agency_nbr, excluded_ind, COUNT(*) AS total_items
FROM   mv_points_by_product_agency
GROUP BY agency_nbr, excluded_ind;
```

### Aggregate 2 — Current: sum of pg_points by agency + asset_product_ent_cd (included only)
```sql
SELECT agency_nbr, asset_product_ent_cd, SUM(pg_points_value) AS total_pg_points
FROM   mv_points_by_product_agency
WHERE  excluded_ind = false
GROUP BY agency_nbr, asset_product_ent_cd;
```

### Aggregate 3 — Year-end items with current AOR + current inclusion status
```sql
SELECT pa.agency_nbr        AS current_agency_nbr,
       inc.excluded_ind      AS current_excluded_ind,
       COUNT(*)              AS item_count
FROM   iif_agency_yearend_snapshot ye
JOIN   policy_aor pa
       ON  pa.agreement_product_number = ye.agreement_product_nbr
JOIN   iif_metric_inclusion inc
       ON  inc.agreement_product_nbr = ye.agreement_product_nbr
       AND inc.asset_id IS NOT DISTINCT FROM ye.asset_id
       AND inc.eff_end_dt = HIGH_DATE
WHERE  ye.production_year = :year
GROUP BY pa.agency_nbr, inc.excluded_ind;
```

### Aggregate 4 — Year-end items with current AOR + current pg_points
```sql
SELECT pa.agency_nbr              AS current_agency_nbr,
       ye.ye_asset_product_ent_cd,
       SUM(pts.pg_points_value)   AS current_total_pg_points
FROM   iif_agency_yearend_snapshot ye
JOIN   policy_aor pa
       ON  pa.agreement_product_number = ye.agreement_product_nbr
JOIN   iif_metric_points pts
       ON  pts.agreement_product_nbr = ye.agreement_product_nbr
       AND pts.asset_id IS NOT DISTINCT FROM ye.asset_id
       AND pts.eff_end_dt = HIGH_DATE
WHERE  ye.production_year = :year
GROUP BY pa.agency_nbr, ye.ye_asset_product_ent_cd;
```

## Year-End Snapshot

### Data Flow

```
Jan 1 job (ShedLock "yearEndSnapshotJob") — MUST run before daily job refreshes MV:
  │
  ├─ 1. Acquire ShedLock "yearEndSnapshotJob"
  ├─ 2. DELETE FROM iif_agency_yearend_snapshot WHERE production_year = <prev year>
  ├─ 3. INSERT INTO iif_agency_yearend_snapshot
  │      SELECT from iif_metric_points + iif_metrics_raw active rows (base tables, not MV)
  └─ 4. Release lock

Then the regular daily job runs and refreshes the MV with Jan 1 data.
```

> **Ordering constraint:** the year-end job must complete before the daily MV refresh runs on Jan 1.
> Enforce via cron timing (year-end job at 00:00, daily job at 02:00) or by having the daily
> job check that the year-end lock has been released.

### Table: `iif_agency_yearend_snapshot`

Stores **item-level** rows (one per `(agreement_product_nbr, asset_id)`) with the year-end values.
Join keys are preserved so aggregates 3 & 4 can look up current AOR and current inclusion/points.
Partitioned by `production_year` — each year's data is physically isolated.

```sql
CREATE TABLE iif_agency_yearend_snapshot (
    id                      BIGSERIAL    PRIMARY KEY,
    production_year         SMALLINT     NOT NULL,
    agreement_product_nbr   VARCHAR(255) NOT NULL,
    asset_id                VARCHAR(255),            -- nullable, NULL-safe joins
    ye_asset_product_ent_cd VARCHAR(64),
    ye_pg_points_value      INTEGER,
    snapshot_taken_at       TIMESTAMPTZ  NOT NULL
) PARTITION BY LIST (production_year);

-- One partition per year, added via migration each year:
CREATE TABLE iif_agency_yearend_snapshot_2025
    PARTITION OF iif_agency_yearend_snapshot FOR VALUES IN (2025);
```

Year-end job populates from base tables directly (not the MV — the MV is aggregated):
```sql
DELETE FROM iif_agency_yearend_snapshot WHERE production_year = :year;

INSERT INTO iif_agency_yearend_snapshot
    (production_year, agreement_product_nbr, asset_id, ye_asset_product_ent_cd, ye_pg_points_value, snapshot_taken_at)
SELECT :year,
       pts.agreement_product_nbr,
       pts.asset_id,
       r.asset_product_ent_cd,
       pts.pg_points_value,
       now()
FROM   iif_metric_points pts
JOIN   iif_metrics_raw r
       ON  r.agreement_product_nbr = pts.agreement_product_nbr
       AND r.asset_id IS NOT DISTINCT FROM pts.asset_id
       AND r.eff_end_dt = HIGH_DATE
WHERE  pts.eff_end_dt = HIGH_DATE;
```

### API

```
POST /api/jobs/yearend-snapshot?year=2025   (optional, defaults to previous year)

202 Accepted  — job started
409 Conflict  — already running
```

---

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
      lock-at-most: PT4H
    yearend-snapshot:
      cron: "0 0 0 1 1 *"   # Jan 1 at midnight
      lock-at-most: PT1H
```

---

## Build Phases

### Phase 1 — Database
1. **V3 migration** — materialized view `mv_points_by_product_agency` joining
   `iif_metric_points` + `iif_metrics_raw` + `iif_metric_inclusion` + `policy_aor`,
   unique index for `CONCURRENTLY` refresh
2. **V4 migration** — `iif_agency_daily_aggregate` result table + ShedLock table
3. **V5 migration** — `iif_agency_yearend_snapshot` partitioned table + first partition (current year − 1)

### Phase 2 — Repository layer
4. `MvRefreshRepository` — `REFRESH MATERIALIZED VIEW CONCURRENTLY`
5. `AgencyAggregateRepository` — `IN` query against MV + upsert to result table
6. Drop the now-redundant `@Query` on `IIifMetricsPgPointsRepository` (MV replaces it)

### Phase 3 — Kafka
7. `AgencyAggregateWork` record
8. New Kafka topic config — `iif.agency-aggregate-work` (producer + consumer beans)
9. `AgencyWorkPublisher` — publishes one message per agency
10. `AgencyAggregateWorkConsumer` — listener, runs per-agency COUNT + SUM, upserts results

### Phase 4 — Job orchestration
11. ShedLock dependency in `build.gradle`
12. `DailyAggregateJob` — `run(LocalDate)`: refresh → query distinct agencies → publish one message per agency
13. `@Scheduled` + `@SchedulerLock` wrapper
14. `YearEndSnapshotJob` — delete prior year rows, insert from MV, `@Scheduled` + `@SchedulerLock`

### Phase 5 — API
15. `JobController` — `POST /api/jobs/daily-aggregate` using `LockingTaskExecutor`
16. `JobController` — `POST /api/jobs/yearend-snapshot?year=` using `LockingTaskExecutor`
17. Bruno requests for both triggers

### Phase 6 — Config + tests
18. `application.yml` additions — topic name, cron expressions, lock durations
19. Unit tests — `DailyAggregateJob`, `AgencyWorkPublisher`, `AgencyAggregateWorkConsumer`, `YearEndSnapshotJob`

---

## Key Design Properties

- **Idempotent** — upsert on `(agency_nbr, job_date, aggregate_type)`; redelivered messages are safe
- **Re-runnable** — API trigger with explicit `jobDate` reruns any past date
- **No barrier needed** — MV refreshed before any messages published; workers read consistent snapshot
- **Horizontally scalable** — add instances, Kafka rebalances partitions automatically
- **ShedLock on both paths** — scheduler and API share same lock name; concurrent triggers safely rejected
- **NULL-safe joins** — all `asset_id` joins use `IS NOT DISTINCT FROM`
- **Year-end snapshot** — Jan 1 job writes MV contents (still reflecting 12/31 data) into partitioned snapshot table before daily refresh overwrites the MV
- **Partition per year** — new `iif_agency_yearend_snapshot_<year>` partition added via Flyway migration each year; queries against a single year never touch other partitions
