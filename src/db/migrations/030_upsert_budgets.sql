-- ============================================================
-- 014_upsert_budgets.sql
-- ============================================================
-- Purpose:
--   1. Deduplicate staged budgets
--   2. Upsert budgets into core table
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
    SELECT *
    FROM (
        SELECT
            sb.*,
            ROW_NUMBER() OVER (
                PARTITION BY sb.budget_id
                ORDER BY sb.ingested_at DESC NULLS LAST
            ) AS rn
        FROM stg_budgets sb
    ) t
    WHERE t.rn = 1
),

-- ----------------------------
-- UPSERT BUDGETS
-- ----------------------------
upserted AS (
    INSERT INTO budgets (
        budget_id,
        budget_uuid,
        name,
        description,
        retired,
        start_date,
        recurring_interval,
        timezone,
        limit_amount,
        overspend_buffer,
        assigned_amount,
        spent_cleared,
        spent_pending,
        spent_total,
        sync_day,
        created_at,
        updated_at,
        source
    )
    SELECT
        d.budget_id,
        d.budget_uuid,
        d.name,
        d.description,
        d.retired,
        d.start_date,
        d.recurring_interval,
        d.timezone,
        COALESCE(d.limit_amount, 0) AS limit_amount,
        d.overspend_buffer,
        d.assigned_amount,
        d.spent_cleared,
        d.spent_pending,
        d.spent_total,
        d.sync_day,
        NOW() AS created_at,
        NOW() AS updated_at,
        d.source
    FROM deduped d
    ON CONFLICT (budget_id)
    DO UPDATE SET
        budget_uuid        = EXCLUDED.budget_uuid,
        name               = EXCLUDED.name,
        description        = EXCLUDED.description,
        retired            = EXCLUDED.retired,
        start_date         = EXCLUDED.start_date,
        recurring_interval = EXCLUDED.recurring_interval,
        timezone           = EXCLUDED.timezone,
        limit_amount       = EXCLUDED.limit_amount,
        overspend_buffer   = EXCLUDED.overspend_buffer,
        assigned_amount    = EXCLUDED.assigned_amount,
        spent_cleared      = EXCLUDED.spent_cleared,
        spent_pending      = EXCLUDED.spent_pending,
        spent_total        = EXCLUDED.spent_total,
        sync_day           = EXCLUDED.sync_day,
        updated_at         = NOW(),
        source             = EXCLUDED.source
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
    'budgets_task',
    'budgets',
    'budgets',
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
