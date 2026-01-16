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
  description TEXT,
  retired BOOLEAN,
  start_date DATE,
  recurring_interval TEXT,
  timezone TEXT,
  limit_amount NUMERIC,
  overspend_buffer NUMERIC,
  assigned_amount NUMERIC,
  spent_cleared NUMERIC,
  spent_pending NUMERIC,
  spent_total NUMERIC,
  sync_day TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  source TEXT
);



-- =========================
-- CARDS
-- =========================
CREATE TABLE IF NOT EXISTS cards (
    card_id TEXT PRIMARY KEY,
    card_uuid TEXT,
    name TEXT,
    user_id TEXT,
    user_uuid TEXT,
    budget_id TEXT,
    budget_uuid TEXT,
    last_four TEXT,
    valid_thru TEXT,
    status TEXT,
    type TEXT,
    share_budget_funds BOOLEAN,
    recurring BOOLEAN,
    recurring_limit NUMERIC,
    current_limit NUMERIC,
    current_spent NUMERIC,
    created_time TIMESTAMP,
    updated_time TIMESTAMP,
    last_synced_day TEXT,
    source TEXT,
    ingested_at TIMESTAMP
);

-- =========================
-- TRANSACTIONS
-- =========================
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    transaction_uuid TEXT,

    occurred_time TIMESTAMP,
    updated_time TIMESTAMP,

    user_id TEXT,
    user_uuid TEXT,
    user_name TEXT,

    merchant_name TEXT,
    raw_merchant_name TEXT,

    card_id TEXT,
    card_uuid TEXT,

    budget_id TEXT,
    budget_uuid TEXT,

    original_amount NUMERIC,
    original_currency TEXT,

    amount_usd NUMERIC,
    exchange_rate NUMERIC,
    fx_source TEXT NOT NULL, 

    idempotency_key TEXT UNIQUE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

CREATE TABLE IF NOT EXISTS stg_budgets (
    budget_id TEXT,
    budget_uuid TEXT,
    name TEXT,
    description TEXT,
    retired BOOLEAN,
    start_date DATE,
    recurring_interval TEXT,
    timezone TEXT,
    limit_amount NUMERIC,
    overspend_buffer NUMERIC,
    assigned_amount NUMERIC,
    spent_cleared NUMERIC,
    spent_pending NUMERIC,
    spent_total NUMERIC,
    sync_day TEXT,
    source TEXT,
    ingested_at TIMESTAMP
);



CREATE TABLE IF NOT EXISTS budget_pipeline_errors (
    id SERIAL PRIMARY KEY,
    budget_id TEXT,
    error_type TEXT,
    error_message TEXT,
    raw_record JSONB,
    sync_day TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_cards (
    card_id TEXT PRIMARY KEY,
    card_uuid TEXT,
    name TEXT,
    user_id TEXT,
    user_uuid TEXT,
    budget_id TEXT,
    budget_uuid TEXT,
    last_four TEXT,
    valid_thru TEXT,
    status TEXT,
    type TEXT,
    share_budget_funds BOOLEAN,
    recurring BOOLEAN,
    recurring_limit NUMERIC,
    current_limit NUMERIC,
    current_spent NUMERIC,
    created_time TIMESTAMP,
    updated_time TIMESTAMP,
    sync_day TEXT,
    source TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS card_pipeline_errors (
    card_id TEXT,
    error_type TEXT,
    error_message TEXT,
    raw_record JSONB,
    sync_day TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_transactions (
    transaction_id TEXT,
    transaction_uuid TEXT,

    occurred_time TIMESTAMP,
    updated_time TIMESTAMP,

    user_id TEXT,
    user_uuid TEXT,
    user_name TEXT,

    merchant_name TEXT,
    raw_merchant_name TEXT,

    card_id TEXT,
    card_uuid TEXT,

    budget_id TEXT,
    budget_uuid TEXT,

    original_amount NUMERIC,
    original_currency TEXT,

    amount_usd NUMERIC,
    exchange_rate NUMERIC,
    fx_source TEXT,

    idempotency_key TEXT,

    raw_payload JSONB,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS transaction_pipeline_state (
    pipeline_name TEXT PRIMARY KEY,
    last_processed_time TIMESTAMP
);


CREATE TABLE IF NOT EXISTS transaction_pipeline_errors (
    id SERIAL PRIMARY KEY,
    transaction_id TEXT,
    error_type TEXT,
    error_message TEXT,
    raw_record JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

