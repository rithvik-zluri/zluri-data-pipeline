-- =========================
-- AGENTS
-- =========================
CREATE TABLE IF NOT EXISTS agents (
    agent_id BIGINT PRIMARY KEY,
    email TEXT,
    name TEXT,
    job_title TEXT,
    language TEXT,
    mobile TEXT,
    phone TEXT,
    time_zone TEXT,
    available BOOLEAN,
    deactivated BOOLEAN,          -- from source, keep as-is
    status TEXT,                  -- NEW: 'active' / 'inactive' (derived)
    focus_mode BOOLEAN,
    agent_operational_status TEXT,
    last_active_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- =========================
-- ADMIN GROUPS
-- =========================
CREATE TABLE IF NOT EXISTS admin_groups (
    group_id BIGINT PRIMARY KEY,
    name TEXT,
    description TEXT,
    type TEXT,
    allow_agents_to_change_availability BOOLEAN,
    agent_availability_status BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- =========================
-- AGENT ↔ GROUP
-- =========================
CREATE TABLE IF NOT EXISTS agent_group_membership (
    agent_id BIGINT REFERENCES agents(agent_id),
    group_id BIGINT REFERENCES admin_groups(group_id),
    PRIMARY KEY (agent_id, group_id)
);

-- =========================
-- ROLES
-- =========================
CREATE TABLE IF NOT EXISTS roles (
    role_id BIGINT PRIMARY KEY,
    name TEXT,
    description TEXT,
    is_default BOOLEAN,
    agent_type INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- =========================
-- BUDGETS
-- =========================
CREATE TABLE IF NOT EXISTS budgets (
    budget_id TEXT PRIMARY KEY,
    budget_uuid TEXT,
    name TEXT,
    amount NUMERIC,
    currency TEXT,
    period TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source TEXT
);

-- =========================
-- CARDS
-- =========================
CREATE TABLE IF NOT EXISTS cards (
    card_id TEXT PRIMARY KEY,
    card_uuid TEXT,
    card_name TEXT,
    card_type TEXT,
    status TEXT,
    agent_id TEXT,
    budget_id TEXT,
    last_four TEXT,
    valid_thru TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source TEXT
);

-- =========================
-- TRANSACTIONS
-- =========================
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BIGINT PRIMARY KEY,
    card_id TEXT REFERENCES cards(card_id),
    agent_id BIGINT REFERENCES agents(agent_id),
    amount NUMERIC,
    currency TEXT,
    merchant TEXT,
    category TEXT,
    transaction_time TIMESTAMP,
    status TEXT,
    source TEXT
);

-- =========================
-- AGENT DETAILS
-- =========================
CREATE TABLE IF NOT EXISTS agent_details (
    agent_id BIGINT PRIMARY KEY REFERENCES agents(agent_id),
    org_agent_id TEXT,
    ticket_scope BIGINT,
    signature TEXT,
    freshchat_agent BOOLEAN,
    is_active BOOLEAN,
    avatar TEXT,
    last_login_at TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agent_role_mapping (
    agent_id BIGINT REFERENCES agents(agent_id),
    role_id BIGINT REFERENCES roles(role_id),
    status TEXT, -- 'assigned' or 'removed'
    assigned_at TIMESTAMP,
    removed_at TIMESTAMP,
    PRIMARY KEY (agent_id, role_id)
);

-- =========================
-- AGENT ROLE HISTORY (AUDIT)
-- =========================
CREATE TABLE IF NOT EXISTS agent_role_history (
    id SERIAL PRIMARY KEY,
    agent_id BIGINT,
    role_id BIGINT,
    action TEXT,
    action_time TIMESTAMP DEFAULT NOW()
);

-- =========================
-- AGENT AVAILABILITY
-- =========================
CREATE TABLE IF NOT EXISTS agent_availability (
    agent_id BIGINT REFERENCES agents(agent_id),
    is_available BOOLEAN,
    available_since TEXT,
    channel TEXT
);

-- =========================
-- STAGING TABLES
-- =========================
CREATE TABLE IF NOT EXISTS stg_agents (LIKE agents INCLUDING ALL);
CREATE TABLE IF NOT EXISTS stg_agent_details (LIKE agent_details INCLUDING ALL);
CREATE TABLE IF NOT EXISTS stg_agent_availability (LIKE agent_availability INCLUDING ALL);

-- =========================
-- AGENT STATUS HISTORY
-- =========================
CREATE TABLE IF NOT EXISTS agent_status_history (
    agent_id BIGINT REFERENCES agents(agent_id),
    sync_date DATE,
    is_active BOOLEAN,
    PRIMARY KEY (agent_id, sync_date)
);

-- =========================
-- AGENT PIPELINE ERRORS
-- =========================
CREATE TABLE IF NOT EXISTS agent_pipeline_errors (
    id SERIAL PRIMARY KEY,
    agent_id BIGINT,
    error_type TEXT,
    error_message TEXT,
    raw_record JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- =========================
-- GROUPS
-- =========================
CREATE TABLE IF NOT EXISTS groups (
    group_id BIGINT PRIMARY KEY,
    name TEXT,
    parent_group_id BIGINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    inactive BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS group_membership (
    group_id BIGINT,
    agent_id BIGINT,
    PRIMARY KEY (group_id, agent_id)
);

CREATE TABLE IF NOT EXISTS stg_groups (
    group_id BIGINT,
    name TEXT,
    parent_group_id BIGINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_group_membership (
    group_id BIGINT,
    agent_id BIGINT
);

CREATE TABLE IF NOT EXISTS stg_roles (
    role_id BIGINT,
    name TEXT,
    description TEXT,
    is_default BOOLEAN,
    agent_type INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_agent_roles (
    agent_id BIGINT,
    role_id BIGINT,
    status TEXT,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_agent_role_mapping (
    agent_id BIGINT,
    role_id BIGINT
);

-- =========================
-- GROUP PIPELINE ERRORS
-- =========================
CREATE TABLE IF NOT EXISTS group_pipeline_errors (
    id SERIAL PRIMARY KEY,
    group_id BIGINT,
    error_type TEXT,
    error_message TEXT,
    raw_record JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- ROLE PIPELINE ERRORS
-- =========================
CREATE TABLE IF NOT EXISTS role_pipeline_errors (
    id SERIAL PRIMARY KEY,
    role_id BIGINT,
    error_type TEXT,
    error_message TEXT,
    raw_record JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- AGENT ROLE PIPELINE ERRORS
-- =========================
CREATE TABLE IF NOT EXISTS agent_role_pipeline_errors (
    id SERIAL PRIMARY KEY,
    agent_id BIGINT,
    role_id BIGINT,
    error_type TEXT,
    error_message TEXT,
    raw_record JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
