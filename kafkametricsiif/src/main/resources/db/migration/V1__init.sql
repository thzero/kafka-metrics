-- =============================================================================
-- V1__init.sql  —  Initial schema for kafka-metrics (PostgreSQL)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Control tables (kafkametricsbase)
-- ---------------------------------------------------------------------------

CREATE TABLE received_record (
    id             BIGSERIAL    PRIMARY KEY,
    message_id     VARCHAR(255) NOT NULL,
    interaction_id VARCHAR(255),
    received_at    TIMESTAMPTZ  NOT NULL,
    CONSTRAINT uq_received_message_id UNIQUE (message_id)
);

CREATE TABLE published_record (
    id             BIGSERIAL    PRIMARY KEY,
    message_id     VARCHAR(255) NOT NULL,
    interaction_id VARCHAR(255),
    published_at   TIMESTAMPTZ  NOT NULL
);

CREATE TABLE dead_letter_record (
    id             BIGSERIAL    PRIMARY KEY,
    message_id     VARCHAR(255),
    interaction_id VARCHAR(255),
    raw_payload    TEXT         NOT NULL,
    reason_code    VARCHAR(64)  NOT NULL,
    failed_at      TIMESTAMPTZ  NOT NULL
);

-- ---------------------------------------------------------------------------
-- Lookup / reference data (kafkametricsiif)
-- ---------------------------------------------------------------------------

CREATE TABLE policy_master (
    id                              BIGSERIAL    PRIMARY KEY,
    agreement_product_number        VARCHAR(255),
    original_policy_effective_date  DATE,
    scenario_cd                     VARCHAR(64),
    asset_product_ent_cd            VARCHAR(64)
);

CREATE INDEX idx_policy_master_agreement_product_number
    ON policy_master (agreement_product_number);

CREATE TABLE policy_aor (
    id                       BIGSERIAL    PRIMARY KEY,
    agreement_product_number VARCHAR(255),
    agency_nbr               VARCHAR(64),
    assigned                 BOOLEAN
);

CREATE INDEX idx_policy_aor_agreement_product_number
    ON policy_aor (agreement_product_number);

CREATE TABLE producer (
    id                       BIGSERIAL    PRIMARY KEY,
    agency_nbr               VARCHAR(64),
    bonus_primary_agency_nbr VARCHAR(64),
    cfm_cd                   VARCHAR(64)
);

CREATE INDEX idx_producer_agency_nbr
    ON producer (agency_nbr);

CREATE TABLE cfm_pg_points (
    id                        BIGSERIAL    PRIMARY KEY,
    cfm_cd                    VARCHAR(64),
    product_family_ent_cd     VARCHAR(64),
    product_sub_family_ent_cd VARCHAR(64),
    asset_product_ent_cd      VARCHAR(64),
    pg_points_value           INTEGER
);

CREATE INDEX idx_cfm_pg_points_cfm_cd
    ON cfm_pg_points (cfm_cd);

-- ---------------------------------------------------------------------------
-- Metrics / SCD2 output tables (kafkametricsiif)
-- ---------------------------------------------------------------------------

CREATE TABLE iif_metrics_raw (
    id                        BIGSERIAL    PRIMARY KEY,
    message_id                VARCHAR(255),
    agreement_product_nbr     VARCHAR(255),
    asset_id                  VARCHAR(255),
    asset_product_ent_cd      VARCHAR(64),
    product_family_ent_cd     VARCHAR(64),
    product_sub_family_ent_cd VARCHAR(64),
    published_dt              BIGINT       NOT NULL,
    eff_begin_dt              TIMESTAMPTZ  NOT NULL,
    eff_end_dt                TIMESTAMPTZ  NOT NULL
);

CREATE INDEX idx_iif_metrics_raw_agreement_product_nbr
    ON iif_metrics_raw (agreement_product_nbr);
CREATE INDEX idx_iif_metrics_raw_asset_id
    ON iif_metrics_raw (asset_id);
-- Partial index: only current (open-ended) rows — keeps index tiny as history grows
CREATE INDEX idx_iif_metrics_raw_current
    ON iif_metrics_raw (agreement_product_nbr, asset_id)
    WHERE eff_end_dt = '2009-01-15 06:31:40+00';

CREATE TABLE iif_metric_points (
    id                       BIGSERIAL    PRIMARY KEY,
    agreement_product_nbr    VARCHAR(255),
    asset_id                 VARCHAR(255),
    published_dt             BIGINT       NOT NULL,
    pg_points_value          INTEGER,
    cfm_code                 VARCHAR(64),
    bonus_primary_agency_nbr VARCHAR(64),
    eff_begin_dt             TIMESTAMPTZ  NOT NULL,
    eff_end_dt               TIMESTAMPTZ  NOT NULL
);

CREATE INDEX idx_iif_metric_points_agreement_product_nbr
    ON iif_metric_points (agreement_product_nbr);
CREATE INDEX idx_iif_metric_points_asset_id
    ON iif_metric_points (asset_id);
-- Partial index: only current rows
CREATE INDEX idx_iif_metric_points_current
    ON iif_metric_points (agreement_product_nbr, asset_id)
    WHERE eff_end_dt = '2009-01-15 06:31:40+00';

CREATE TABLE iif_metric_inclusion (
    id                    BIGSERIAL    PRIMARY KEY,
    agreement_product_nbr VARCHAR(255),
    asset_id              VARCHAR(255),
    published_dt          BIGINT       NOT NULL,
    excluded_ind          BOOLEAN      NOT NULL,
    eff_begin_dt          TIMESTAMPTZ  NOT NULL,
    eff_end_dt            TIMESTAMPTZ  NOT NULL
);

CREATE INDEX idx_iif_metric_inclusion_agreement_product_nbr
    ON iif_metric_inclusion (agreement_product_nbr);
CREATE INDEX idx_iif_metric_inclusion_asset_id
    ON iif_metric_inclusion (asset_id);
-- Partial index: only current rows
CREATE INDEX idx_iif_metric_inclusion_current
    ON iif_metric_inclusion (agreement_product_nbr, asset_id)
    WHERE eff_end_dt = '2009-01-15 06:31:40+00';
