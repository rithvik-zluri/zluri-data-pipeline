-- AGENTS
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
    deactivated BOOLEAN,
    focus_mode BOOLEAN,
    agent_operational_status TEXT,
    last_active_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- ADMIN GROUPS
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

-- AGENT ↔ GROUP
CREATE TABLE IF NOT EXISTS agent_group_membership (
    agent_id BIGINT REFERENCES agents(agent_id),
    group_id BIGINT REFERENCES admin_groups(group_id),
    PRIMARY KEY (agent_id, group_id)
);

-- ROLES
CREATE TABLE IF NOT EXISTS roles (
    role_id BIGINT PRIMARY KEY,
    name TEXT,
    description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- BUDGETS
CREATE TABLE IF NOT EXISTS budgets (
    budget_id TEXT PRIMARY KEY,
    name TEXT,
    amount NUMERIC,
    currency TEXT,
    period TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source TEXT
);

-- CARDS
CREATE TABLE IF NOT EXISTS cards (
    card_id BIGINT PRIMARY KEY,
    card_number TEXT,
    card_type TEXT,
    status TEXT,
    agent_id BIGINT REFERENCES agents(agent_id),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source TEXT
);

-- TRANSACTIONS
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BIGINT PRIMARY KEY,
    card_id BIGINT REFERENCES cards(card_id),
    agent_id BIGINT REFERENCES agents(agent_id),
    amount NUMERIC,
    currency TEXT,
    merchant TEXT,
    category TEXT,
    transaction_time TIMESTAMP,
    status TEXT,
    source TEXT
);

-- AGENT DETAILS
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


-- AGENT ROLE
CREATE TABLE IF NOT EXISTS agent_role_mapping (
    agent_id BIGINT REFERENCES agents(agent_id),
    role_id BIGINT REFERENCES roles(role_id),
    PRIMARY KEY (agent_id, role_id)
);


CREATE TABLE IF NOT EXISTS agent_availability (
    agent_id BIGINT REFERENCES agents(agent_id),
    is_available BOOLEAN,
    available_since TEXT,
    channel TEXT
);

-- STAGING TABLES
CREATE TABLE IF NOT EXISTS stg_agents (LIKE agents INCLUDING ALL);
CREATE TABLE IF NOT EXISTS stg_agent_details (LIKE agent_details INCLUDING ALL);
CREATE TABLE IF NOT EXISTS stg_agent_availability (LIKE agent_availability INCLUDING ALL);

-- =========================
-- AGENT STATUS HISTORY (INACTIVE TRACKING)
-- =========================
CREATE TABLE IF NOT EXISTS agent_status_history (
    agent_id BIGINT REFERENCES agents(agent_id),
    sync_date DATE,
    is_active BOOLEAN,
    PRIMARY KEY (agent_id, sync_date)
);
