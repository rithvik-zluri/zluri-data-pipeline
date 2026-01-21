-- ============================================================
-- 012_upsert_agent_roles.sql
-- ============================================================
-- Purpose:
--   1. Upsert current agent-role assignments
--   2. Mark removed roles
--   3. Persist role assignment history (Atomic & Corrected)
--   4. Capture pipeline metrics with Prefect context
-- ============================================================

BEGIN;

-- ----------------------------
-- SINGLE ATOMIC QUERY CHAIN
-- ----------------------------
WITH context AS (
    SELECT
        current_setting('app.flow_run_id', true) AS flow_run_id,
        current_setting('app.day', true)         AS day,
        NOW()                                    AS started_at
),

-- 1. Upsert & Capture Changes
upserted AS (
    INSERT INTO agent_role_mapping (
        agent_id,
        role_id,
        status,
        assigned_at,
        updated_at
    )
    SELECT
        agent_id,
        role_id,
        'assigned',
        NOW(),
        NOW()
    FROM stg_agent_roles
    ON CONFLICT (agent_id, role_id)
    DO UPDATE SET
        status      = 'assigned',
        assigned_at = COALESCE(agent_role_mapping.assigned_at, NOW()),
        removed_at  = NULL,
        updated_at  = NOW()
    RETURNING agent_id, role_id, (xmax = 0) AS inserted
),

-- 2. Mark Removed
removed AS (
    UPDATE agent_role_mapping arm
    SET
        status     = 'removed',
        removed_at = NOW(),
        updated_at = NOW()
    WHERE NOT EXISTS (
        SELECT 1
        FROM stg_agent_roles s
        WHERE s.agent_id = arm.agent_id
          AND s.role_id  = arm.role_id
    )
    AND arm.status != 'removed'
    RETURNING agent_id, role_id
),

-- 3. Calculate Metrics
metrics AS (
    SELECT
        (SELECT COUNT(*) FROM upserted WHERE inserted)     AS rows_inserted,
        (SELECT COUNT(*) FROM upserted WHERE NOT inserted) AS rows_updated,
        (SELECT COUNT(*) FROM removed)                     AS rows_deleted
),

-- 4. Log History: Assigned (Only new insertions)
hist_assigned AS (
    INSERT INTO agent_role_history (agent_id, role_id, action)
    SELECT agent_id, role_id, 'assigned'
    FROM upserted
    WHERE inserted = true
    RETURNING 1
),

-- 5. Log History: Removed
hist_removed AS (
    INSERT INTO agent_role_history (agent_id, role_id, action)
    SELECT agent_id, role_id, 'removed'
    FROM removed
    RETURNING 1
)

-- 6. Log Pipeline Metrics & Trigger CTEs
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
    'agent_roles_task',
    'agent_roles',
    'agent_role_mapping',
    c.day,

    m.rows_inserted,
    m.rows_updated,
    m.rows_deleted,
    m.rows_inserted + m.rows_updated + m.rows_deleted,

    'success',
    NULL,
    c.started_at,
    clock_timestamp(),
    EXTRACT(EPOCH FROM clock_timestamp() - c.started_at)
FROM metrics m
CROSS JOIN context c
CROSS JOIN (SELECT COUNT(*) FROM hist_assigned) ha
CROSS JOIN (SELECT COUNT(*) FROM hist_removed) hr;

COMMIT;
