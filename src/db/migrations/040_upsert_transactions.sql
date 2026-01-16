-- =========================================
-- 040_UPSERT_TRANSACTIONS.SQL
-- =========================================

BEGIN;

-- =====================================================
-- 1. UPSERT INTO MAIN TRANSACTIONS TABLE (DEDUP SAFE)
-- =====================================================

WITH deduped AS (
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

        st.idempotency_key,                 -- 🔥 ADDED
        COALESCE(st.ingested_at, CURRENT_TIMESTAMP) AS ingested_at
    FROM stg_transactions st
    WHERE st.transaction_id IS NOT NULL
    ORDER BY st.transaction_id, st.updated_time DESC NULLS LAST
)

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

    idempotency_key,                      -- 🔥 ADDED
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

    d.idempotency_key,                    -- 🔥 ADDED
    d.ingested_at
FROM deduped d

ON CONFLICT (transaction_id)
DO UPDATE SET
    transaction_uuid   = EXCLUDED.transaction_uuid,
    occurred_time      = EXCLUDED.occurred_time,
    updated_time       = EXCLUDED.updated_time,

    user_id            = EXCLUDED.user_id,
    user_uuid          = EXCLUDED.user_uuid,
    user_name          = EXCLUDED.user_name,

    merchant_name      = EXCLUDED.merchant_name,
    raw_merchant_name  = EXCLUDED.raw_merchant_name,

    card_id            = EXCLUDED.card_id,
    card_uuid          = EXCLUDED.card_uuid,

    budget_id          = EXCLUDED.budget_id,
    budget_uuid        = EXCLUDED.budget_uuid,

    original_amount    = EXCLUDED.original_amount,
    original_currency  = EXCLUDED.original_currency,

    amount_usd         = EXCLUDED.amount_usd,
    exchange_rate      = EXCLUDED.exchange_rate,

    idempotency_key    = EXCLUDED.idempotency_key,   -- 🔥 ADDED
    ingested_at        = EXCLUDED.ingested_at

WHERE
    transactions.updated_time IS NULL
    OR EXCLUDED.updated_time >= transactions.updated_time;


-- =====================================================
-- 2. UPDATE PIPELINE STATE
-- =====================================================

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
        COALESCE(EXCLUDED.last_processed_time, transaction_pipeline_state.last_processed_time)
    );


-- =====================================================
-- 3. CLEANUP STAGING
-- =====================================================

TRUNCATE TABLE stg_transactions;

COMMIT;
