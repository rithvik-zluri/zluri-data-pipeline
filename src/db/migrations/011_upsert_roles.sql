-- ============================================================
-- 011_upsert_roles.sql
-- ============================================================
-- Purpose:
--   1. Upsert roles from stg_roles into roles
--   2. Capture insert/update metrics
--   3. Persist pipeline metrics with Prefect context
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
-- UPSERT ROLES
-- ----------------------------
upserted AS (
    INSERT INTO roles (
        role_id,
        name,
        description,
        is_default,
        agent_type,
        created_at,
        updated_at
    )
    SELECT
        role_id,
        name,
        description,
        is_default,
        agent_type,
        created_at::timestamp,
        updated_at::timestamp
    FROM stg_roles
    ON CONFLICT (role_id)
    DO UPDATE SET
        name        = EXCLUDED.name,
        description = EXCLUDED.description,
        is_default  = EXCLUDED.is_default,
        agent_type  = EXCLUDED.agent_type,
        updated_at  = EXCLUDED.updated_at
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
    'roles_task',
    'roles',
    'roles',
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
