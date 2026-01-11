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
ON CONFLICT (role_id) DO UPDATE
SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    is_default = EXCLUDED.is_default,
    agent_type = EXCLUDED.agent_type,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;
