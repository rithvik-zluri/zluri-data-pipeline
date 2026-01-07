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
    deactivated BOOLEAN,
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
-- AGENT ↔ GROUP MAPPING
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
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- =========================
-- BUDGETS
-- =========================
CREATE TABLE IF NOT EXISTS budgets (
    budget_id BIGINT PRIMARY KEY,
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
    card_id BIGINT PRIMARY KEY,
    card_number TEXT,
    card_type TEXT,
    status TEXT,
    agent_id BIGINT REFERENCES agents(agent_id),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source TEXT
);

-- =========================
-- TRANSACTIONS
-- =========================
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

-- =========================
-- AGENT DETAILS (EXTENDED)
-- =========================
CREATE TABLE IF NOT EXISTS agent_details (
    agent_id BIGINT PRIMARY KEY REFERENCES agents(agent_id),
    org_agent_id TEXT,
    ticket_scope INT,
    signature TEXT,
    freshchat_agent BOOLEAN,
    availability JSONB,
    contact JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
