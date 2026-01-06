CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    email VARCHAR UNIQUE NOT NULL,
    full_name VARCHAR,
    status VARCHAR DEFAULT 'active',
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE groups (
    group_id VARCHAR PRIMARY KEY,
    group_name VARCHAR NOT NULL,
    parent_group_id VARCHAR,
    status VARCHAR DEFAULT 'active',
    FOREIGN KEY (parent_group_id) REFERENCES groups(group_id)
);

CREATE TABLE user_group_membership (
    user_id VARCHAR REFERENCES users(user_id),
    group_id VARCHAR REFERENCES groups(group_id),
    PRIMARY KEY (user_id, group_id)
);

CREATE TABLE transactions (
    transaction_id VARCHAR PRIMARY KEY,
    user_id VARCHAR REFERENCES users(user_id),
    original_amount NUMERIC,
    original_currency VARCHAR(5),
    amount_usd NUMERIC,
    transaction_date DATE,
    processed_at TIMESTAMP
);
