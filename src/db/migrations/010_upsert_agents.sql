-- =========================
-- UPSERT AGENTS
-- =========================
INSERT INTO agents
SELECT * FROM stg_agents
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
    deactivated = EXCLUDED.deactivated,
    focus_mode = EXCLUDED.focus_mode,
    agent_operational_status = EXCLUDED.agent_operational_status,
    last_active_at = EXCLUDED.last_active_at,
    updated_at = EXCLUDED.updated_at;

-- =========================
-- UPSERT AGENT DETAILS
-- =========================
INSERT INTO agent_details
SELECT * FROM stg_agent_details
ON CONFLICT (agent_id)
DO UPDATE SET
    org_agent_id = EXCLUDED.org_agent_id,
    ticket_scope = EXCLUDED.ticket_scope,
    signature = EXCLUDED.signature,
    freshchat_agent = EXCLUDED.freshchat_agent,
    is_active = EXCLUDED.is_active,
    avatar = EXCLUDED.avatar,
    last_login_at = EXCLUDED.last_login_at,
    updated_at = EXCLUDED.updated_at;

-- =========================
-- REBUILD AVAILABILITY
-- =========================
DELETE FROM agent_availability
USING stg_agent_availability s
WHERE agent_availability.agent_id = s.agent_id;

INSERT INTO agent_availability
SELECT * FROM stg_agent_availability;

-- =========================
-- MARK INACTIVE AGENTS
-- =========================
UPDATE agents
SET deactivated = true,
    updated_at = NOW()
WHERE agent_id NOT IN (
    SELECT agent_id FROM stg_agents
);
