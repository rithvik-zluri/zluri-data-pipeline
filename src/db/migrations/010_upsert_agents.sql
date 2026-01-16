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
    email = EXCLUDED.email,
    name = EXCLUDED.name,
    job_title = EXCLUDED.job_title,
    language = EXCLUDED.language,
    mobile = EXCLUDED.mobile,
    phone = EXCLUDED.phone,
    time_zone = EXCLUDED.time_zone,
    available = EXCLUDED.available,
    available_since = EXCLUDED.available_since,
    deactivated = EXCLUDED.deactivated,
    status = EXCLUDED.status,
    focus_mode = EXCLUDED.focus_mode,
    agent_operational_status = EXCLUDED.agent_operational_status,
    last_active_at = EXCLUDED.last_active_at,
    last_login_at = EXCLUDED.last_login_at,
    org_agent_id = EXCLUDED.org_agent_id,
    ticket_scope = EXCLUDED.ticket_scope,
    signature = EXCLUDED.signature,
    freshchat_agent = EXCLUDED.freshchat_agent,
    updated_at = EXCLUDED.updated_at;

-- =========================
-- AGENT STATUS HISTORY SNAPSHOT
-- =========================
INSERT INTO agent_status_history (
    agent_id,
    sync_date,
    is_active
)
SELECT
    agent_id,
    DATE :'sync_date' AS sync_date,
    (status = 'active') AS is_active
FROM agents
ON CONFLICT (agent_id, sync_date)
DO UPDATE SET
    is_active = EXCLUDED.is_active;
