-- ============================================================
-- 015_upsert_cards.sql
-- ============================================================
-- Purpose:
--   1. Deduplicate staged cards
--   2. Upsert cards into core table
--   3. Capture insert/update metrics
--   4. Persist pipeline metrics with Prefect context
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
    SELECT DISTINCT ON (card_id)
        card_id,
        card_uuid,
        name,
        user_id,
        user_uuid,
        budget_id,
        budget_uuid,
        last_four,
        valid_thru,
        status,
        type,
        share_budget_funds,
        recurring,
        recurring_limit,
        current_limit,
        current_spent,
        created_time,
        updated_time,
        sync_day      AS last_synced_day,
        source,
        ingested_at
    FROM stg_cards
    ORDER BY card_id, ingested_at DESC
),

-- ----------------------------
-- UPSERT CARDS
-- ----------------------------
upserted AS (
    INSERT INTO cards (
        card_id,
        card_uuid,
        name,
        user_id,
        user_uuid,
        budget_id,
        budget_uuid,
        last_four,
        valid_thru,
        status,
        type,
        share_budget_funds,
        recurring,
        recurring_limit,
        current_limit,
        current_spent,
        created_time,
        updated_time,
        last_synced_day,
        source,
        ingested_at
    )
    SELECT
        card_id,
        card_uuid,
        name,
        user_id,
        user_uuid,
        budget_id,
        budget_uuid,
        last_four,
        valid_thru,
        status,
        type,
        share_budget_funds,
        recurring,
        recurring_limit,
        current_limit,
        current_spent,
        created_time,
        updated_time,
        last_synced_day,
        source,
        ingested_at
    FROM deduped
    ON CONFLICT (card_id)
    DO UPDATE SET
        card_uuid          = EXCLUDED.card_uuid,
        name               = EXCLUDED.name,
        user_id            = EXCLUDED.user_id,
        user_uuid          = EXCLUDED.user_uuid,
        budget_id          = EXCLUDED.budget_id,
        budget_uuid        = EXCLUDED.budget_uuid,
        last_four          = EXCLUDED.last_four,
        valid_thru         = EXCLUDED.valid_thru,
        status             = EXCLUDED.status,
        type               = EXCLUDED.type,
        share_budget_funds = EXCLUDED.share_budget_funds,
        recurring          = EXCLUDED.recurring,
        recurring_limit    = EXCLUDED.recurring_limit,
        current_limit      = EXCLUDED.current_limit,
        current_spent      = EXCLUDED.current_spent,
        created_time       = EXCLUDED.created_time,
        updated_time       = EXCLUDED.updated_time,
        last_synced_day    = EXCLUDED.last_synced_day,
        source             = EXCLUDED.source,
        ingested_at        = EXCLUDED.ingested_at
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
    'cards_task',
    'cards',
    'cards',
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

COMMIT;
