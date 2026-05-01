-- ---------------------------------------------------------------------------
-- V2: Covering indexes for the PgPoints aggregate query + unique constraints
--     on active SCD2 rows.
--
-- asset_id is nullable. NULLS NOT DISTINCT (PostgreSQL 15+) ensures that
-- (agreement_product_nbr, NULL) is treated as a unique combination, so only
-- one active row per asset can exist even when asset_id is absent.
-- ---------------------------------------------------------------------------

-- Unique constraint: one active row per (agreement_product_nbr, asset_id)
-- on iif_metric_points.  NULLS NOT DISTINCT means two NULLs are considered
-- equal — correct for SCD2 where NULL asset_id is a valid business key.
CREATE UNIQUE INDEX uq_iif_metric_points_active
    ON iif_metric_points (agreement_product_nbr, asset_id)
    NULLS NOT DISTINCT
    WHERE eff_end_dt = '2009-01-15 06:31:40+00';

-- Same constraint for iif_metric_inclusion.
CREATE UNIQUE INDEX uq_iif_metric_inclusion_active
    ON iif_metric_inclusion (agreement_product_nbr, asset_id)
    NULLS NOT DISTINCT
    WHERE eff_end_dt = '2009-01-15 06:31:40+00';

-- ---------------------------------------------------------------------------
-- Covering indexes for MV refresh performance
--
-- These indexes are NOT used by queries against the materialized view —
-- those hit the pre-computed MV table directly.  They speed up the
-- REFRESH MATERIALIZED VIEW statement, which re-runs the underlying
-- aggregate query against the base tables once per daily job cycle.
-- ---------------------------------------------------------------------------

-- iif_metric_points: include pg_points_value so the refresh SUM never touches the heap
CREATE INDEX idx_iif_metric_points_agg
    ON iif_metric_points (agreement_product_nbr, asset_id)
    INCLUDE (pg_points_value)
    WHERE eff_end_dt = '2009-01-15 06:31:40+00';

-- iif_metrics_raw: include asset_product_ent_cd used in the MV GROUP BY
CREATE INDEX idx_iif_metrics_raw_agg
    ON iif_metrics_raw (agreement_product_nbr, asset_id, asset_product_ent_cd)
    WHERE eff_end_dt = '2009-01-15 06:31:40+00';

