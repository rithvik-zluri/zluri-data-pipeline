-- ============================================================
-- 010_upsert_agents.sql
-- ============================================================
-- Purpose:
--   1. Upsert agents from stg_agents into agents
--   2. Prevent older data from overwriting newer data
--   3. Capture insert/update metrics
--   4. Snapshot agent active status per day
--   5. Persist pipeline metrics with Prefect context
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
-- UPSERT AGENTS (DEFENSIVE)
-- ----------------------------
upserted AS (
    INSERT INTO agents (
        agent_id,
        email,
        name,
        job_title,
        language,
        mobile,
        phone,
        time_zone,
        available,
        available_since,
        deactivated,
        status,
        focus_mode,
        agent_operational_status,
        last_active_at,
        last_login_at,
        org_agent_id,
        ticket_scope,
        signature,
        freshchat_agent,
        created_at,
        updated_at
    )
    SELECT
        agent_id,
        email,
        name,
        job_title,
        language,
        mobile,
        phone,
        time_zone,
        available,
        available_since,
        deactivated,
        status,
        focus_mode,
        agent_operational_status,
        last_active_at,
        last_login_at,
        org_agent_id,
        ticket_scope,
        signature,
        freshchat_agent,
        created_at,
        updated_at
    FROM stg_agents
    ON CONFLICT (agent_id)
    DO UPDATE SET
        email                    = EXCLUDED.email,
        name                     = EXCLUDED.name,
        job_title                = EXCLUDED.job_title,
        language                 = EXCLUDED.language,
        mobile                   = EXCLUDED.mobile,
        phone                    = EXCLUDED.phone,
        time_zone                = EXCLUDED.time_zone,
        available                = EXCLUDED.available,
        available_since          = EXCLUDED.available_since,
        deactivated              = EXCLUDED.deactivated,
        status                   = EXCLUDED.status,
        focus_mode               = EXCLUDED.focus_mode,
        agent_operational_status = EXCLUDED.agent_operational_status,
        last_active_at           = EXCLUDED.last_active_at,
        last_login_at            = EXCLUDED.last_login_at,
        org_agent_id             = EXCLUDED.org_agent_id,
        ticket_scope             = EXCLUDED.ticket_scope,
        signature                = EXCLUDED.signature,
        freshchat_agent          = EXCLUDED.freshchat_agent,

        -- 🔒 Prevent time-travel overwrites
        updated_at = GREATEST(
            agents.updated_at,
            EXCLUDED.updated_at
        )

    -- 🔒 Only allow overwrite if incoming record is newer or equal
    WHERE EXCLUDED.updated_at >= agents.updated_at

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
    'agents_task',
    'agents',
    'agents',
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
-- AGENT STATUS HISTORY (DEFENSIVE)
-- ----------------------------
INSERT INTO agent_status_history (
    agent_id,
    sync_date,
    is_active
)
SELECT
    agent_id,
    COALESCE(updated_at, NOW())::date,
    (status = 'active')
FROM agents
ON CONFLICT (agent_id, sync_date)
DO UPDATE
SET is_active = EXCLUDED.is_active;

COMMIT;
