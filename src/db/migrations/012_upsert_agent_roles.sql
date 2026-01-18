-- ============================================================
-- 012_upsert_agent_roles.sql
-- ============================================================
-- Purpose:
--   1. Upsert current agent-role assignments
--   2. Mark removed roles
--   3. Persist role assignment history
--   4. Capture pipeline metrics with Prefect context
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
-- UPSERT CURRENT ROLE STATE
-- ----------------------------
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
    RETURNING (xmax = 0) AS inserted
),

-- ----------------------------
-- MARK REMOVED ROLES
-- ----------------------------
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
    RETURNING 1
),

-- ----------------------------
-- METRICS
-- ----------------------------
metrics AS (
    SELECT
        (SELECT COUNT(*) FROM upserted WHERE inserted)     AS rows_inserted,
        (SELECT COUNT(*) FROM upserted WHERE NOT inserted) AS rows_updated,
        (SELECT COUNT(*) FROM removed)                     AS rows_deleted
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
    f.finished_at,
    EXTRACT(EPOCH FROM f.finished_at - c.started_at)
FROM metrics m
CROSS JOIN context c
CROSS JOIN finished f;

-- ----------------------------
-- HISTORY: ASSIGNED
-- ----------------------------
INSERT INTO agent_role_history (
    agent_id,
    role_id,
    action
)
SELECT
    agent_id,
    role_id,
    'assigned'
FROM stg_agent_roles;

-- ----------------------------
-- HISTORY: REMOVED
-- ----------------------------
INSERT INTO agent_role_history (
    agent_id,
    role_id,
    action
)
SELECT
    agent_id,
    role_id,
    'removed'
FROM agent_role_mapping
WHERE status = 'removed';

COMMIT;
