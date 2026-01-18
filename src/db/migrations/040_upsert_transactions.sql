-- ============================================================
-- 040_upsert_transactions.sql
-- ============================================================
-- Purpose:
--   1. Deduplicate staged transactions
--   2. Upsert transactions into core table
--   3. Capture insert/update metrics
--   4. Update transaction pipeline state
--   5. Cleanup staging table
-- ============================================================

BEGIN;

-- ----------------------------
-- Capture execution context
-- ----------------------------
WITH context AS (
    SELECT
        current_setting('app.flow_run_id', true) AS flow_run_id,
        current_setting('app.day', true)         AS day,
        NOW()                                    AS started_at
),

-- ----------------------------
-- DEDUPLICATE STAGING
-- ----------------------------
deduped AS (
    SELECT DISTINCT ON (st.transaction_id)
        st.transaction_id,
        st.transaction_uuid,

        st.occurred_time,
        st.updated_time,

        st.user_id,
        st.user_uuid,
        st.user_name,

        st.merchant_name,
        st.raw_merchant_name,

        st.card_id,
        st.card_uuid,

        st.budget_id,
        st.budget_uuid,

        st.original_amount,
        st.original_currency,

        st.amount_usd,
        st.exchange_rate,
        st.fx_source,

        st.idempotency_key,
        COALESCE(st.ingested_at, CURRENT_TIMESTAMP) AS ingested_at
    FROM stg_transactions st
    WHERE st.transaction_id IS NOT NULL
    ORDER BY st.transaction_id, st.updated_time DESC NULLS LAST
),

-- ----------------------------
-- UPSERT TRANSACTIONS
-- ----------------------------
upserted AS (
    INSERT INTO transactions (
        transaction_id,
        transaction_uuid,

        occurred_time,
        updated_time,

        user_id,
        user_uuid,
        user_name,

        merchant_name,
        raw_merchant_name,

        card_id,
        card_uuid,

        budget_id,
        budget_uuid,

        original_amount,
        original_currency,

        amount_usd,
        exchange_rate,
        fx_source,

        idempotency_key,
        ingested_at
    )
    SELECT
        d.transaction_id,
        d.transaction_uuid,

        d.occurred_time,
        d.updated_time,

        d.user_id,
        d.user_uuid,
        d.user_name,

        d.merchant_name,
        d.raw_merchant_name,

        d.card_id,
        d.card_uuid,

        d.budget_id,
        d.budget_uuid,

        d.original_amount,
        d.original_currency,

        d.amount_usd,
        d.exchange_rate,
        d.fx_source,

        d.idempotency_key,
        d.ingested_at
    FROM deduped d
    ON CONFLICT (transaction_id)
    DO UPDATE SET
        transaction_uuid  = EXCLUDED.transaction_uuid,
        occurred_time     = EXCLUDED.occurred_time,
        updated_time      = EXCLUDED.updated_time,

        user_id           = EXCLUDED.user_id,
        user_uuid         = EXCLUDED.user_uuid,
        user_name         = EXCLUDED.user_name,

        merchant_name     = EXCLUDED.merchant_name,
        raw_merchant_name = EXCLUDED.raw_merchant_name,

        card_id           = EXCLUDED.card_id,
        card_uuid         = EXCLUDED.card_uuid,

        budget_id         = EXCLUDED.budget_id,
        budget_uuid       = EXCLUDED.budget_uuid,

        original_amount   = EXCLUDED.original_amount,
        original_currency = EXCLUDED.original_currency,

        amount_usd        = EXCLUDED.amount_usd,
        exchange_rate     = EXCLUDED.exchange_rate,
        fx_source         = EXCLUDED.fx_source,

        idempotency_key   = EXCLUDED.idempotency_key,
        ingested_at       = EXCLUDED.ingested_at
    WHERE
        transactions.updated_time IS NULL
        OR EXCLUDED.updated_time >= transactions.updated_time
    RETURNING (xmax = 0) AS inserted
),

-- ----------------------------
-- METRICS
-- ----------------------------
metrics AS (
    SELECT
        COUNT(*) FILTER (WHERE inserted)     AS rows_inserted,
        COUNT(*) FILTER (WHERE NOT inserted) AS rows_updated
    FROM upserted
),

-- ----------------------------
-- FINISH TIMING
-- ----------------------------
finished AS (
    SELECT NOW() AS finished_at
)

-- ----------------------------
-- PIPELINE METRICS
-- ----------------------------
INSERT INTO pipeline_metrics (
    flow_run_id,
    task_name,
    pipeline_name,
    table_name,
    day,
    rows_inserted,
    rows_updated,
    rows_deleted,
    rows_affected,
    status,
    error_message,
    started_at,
    finished_at,
    duration_seconds
)
SELECT
    c.flow_run_id,
    'transactions_task',
    'transactions',
    'transactions',
    c.day,

    m.rows_inserted,
    m.rows_updated,
    0,
    m.rows_inserted + m.rows_updated,

    'success',
    NULL,
    c.started_at,
    f.finished_at,
    EXTRACT(EPOCH FROM f.finished_at - c.started_at)
FROM metrics m
CROSS JOIN context c
CROSS JOIN finished f;

-- ----------------------------
-- UPDATE PIPELINE STATE
-- ----------------------------
INSERT INTO transaction_pipeline_state (
    pipeline_name,
    last_processed_time
)
SELECT
    'transactions' AS pipeline_name,
    MAX(updated_time) AS last_processed_time
FROM stg_transactions
WHERE updated_time IS NOT NULL
ON CONFLICT (pipeline_name)
DO UPDATE SET
    last_processed_time = GREATEST(
        transaction_pipeline_state.last_processed_time,
        COALESCE(EXCLUDED.last_processed_time,
                 transaction_pipeline_state.last_processed_time)
    );

-- ----------------------------
-- CLEANUP STAGING
-- ----------------------------
TRUNCATE TABLE stg_transactions;

COMMIT;
