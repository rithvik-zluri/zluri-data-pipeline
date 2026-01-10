-- =========================
-- UPSERT CURRENT ROLE STATE
-- =========================
INSERT INTO agent_role_mapping (agent_id, role_id, status, assigned_at, updated_at)
SELECT
    agent_id,
    role_id,
    'assigned',
    NOW(),
    NOW()
FROM stg_agent_roles
ON CONFLICT (agent_id, role_id)
DO UPDATE SET
    status = 'assigned',
    assigned_at = COALESCE(agent_role_mapping.assigned_at, NOW()),
    removed_at = NULL,
    updated_at = NOW();



-- =========================
-- MARK REMOVED ROLES
-- =========================
UPDATE agent_role_mapping arm
SET
    status = 'removed',
    removed_at = NOW(),
    updated_at = NOW()
WHERE NOT EXISTS (
    SELECT 1
    FROM stg_agent_roles s
    WHERE s.agent_id = arm.agent_id
      AND s.role_id = arm.role_id
)
AND arm.status != 'removed';



-- =========================
-- HISTORY: ASSIGNED
-- =========================
INSERT INTO agent_role_history (agent_id, role_id, action)
SELECT agent_id, role_id, 'assigned'
FROM stg_agent_roles;


-- =========================
-- HISTORY: REMOVED
-- =========================
INSERT INTO agent_role_history (agent_id, role_id, action)
SELECT agent_id, role_id, 'removed'
FROM agent_role_mapping
WHERE status = 'removed';
