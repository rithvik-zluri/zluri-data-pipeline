# Agents Pipeline Documentation

## Overview

The **Agents Pipeline** is responsible for ingesting, validating, transforming, and loading agent data from Freshchat/Zluri API JSON responses into PostgreSQL.

It follows a:

**staging → validation → deduplication → idempotent load → upsert → history snapshot**

pattern to ensure correctness, traceability, and safe re-runs.

This pipeline is designed to be:

- **Idempotent** – safe to re-run for the same sync day  
- **State-aware** – correctly detects deactivated agents  
- **Validated** – invalid records are captured in an error table  
- **Deduplicated** – within-batch duplicates are removed  
- **Upsert-driven** – final tables always reflect the latest state  
- **Historized** – agent status changes are tracked over time  

---

## High-Level Flow

```text
API JSON (agent_details + agents folders)
        ↓
read_agent_details_raw()      read_agents_raw()
        ↓                             ↓
        └────────────── transform_agents() ──────────────┐
                        ├── validation
                        ├── deactivated logic
                        ├── deduplication
                        ↓
                  stg_agents (staging table)
                        ↓
                010_upsert_agents.sql
                        ↓
                     agents (final table)
                        ↓
             agent_status_history (daily snapshot)
```

### Why We Read agent_details and agents Separately

agent_details → master list of all agents ever known

agents folder → agents active in the current sync

By comparing both, we can correctly detect deactivated agents.

If an agent is present in agent_details but missing in agents/dayX, it means:

Freshchat has marked the agent inactive

This is state management across days.
Without this, deactivation cannot be detected correctly.

### Why Deactivated Is Calculated in Spark (Not SQL)

deactivated = is_active IS NULL
Reasoning
Missing from agents/dayX ⇒ deactivated

Doing this in Spark:

- keeps SQL simple

- avoids complex NOT IN queries

- ensures correct day1 → day2 transition

👉 Business logic belongs in Spark.

### Why We Write to stg_agents First (Staging)

Staging tables give:

- Idempotency – safe re-runs

- Debuggability – SELECT * FROM stg_agents

- Separation of concerns – Spark = transform, SQL = persistence


### Why We Use INSERT ... ON CONFLICT (Upsert)

We need:

Day 1 → INSERT

Day 2 → UPDATE existing rows

Day 3 → UPDATE again

Pattern
```

INSERT INTO agents (...)
SELECT ... FROM stg_agents
ON CONFLICT (agent_id)
DO UPDATE SET ...
```
### Result

1. Incremental

2. Idempotent

3. Safe for re-runs

4. Always reflects latest state

### Why We Have agent_status_history

The agents table only stores latest state.
History is lost on every update.

**Solution** is to
Store daily snapshots:

### Why `sync_date` Is Parameterized

**Reasoning**

- Production → CURRENT_DATE

- Testing / simulation → pass date manually

**This allows:**

- Proper day1 → day2 → day3 simulation

- No overwriting history

- Clean backfills

## Table Definitions

#### `stg_agents` (Staging)

| Column                   | Type      | Description                      |
|--------------------------|-----------|----------------------------------|
| agent_id                 | BIGINT    | Agent ID from source             |
| email                    | TEXT      | Agent email                      |
| name                     | TEXT      | Agent name                       |
| job_title                | TEXT      | Job title                        |
| language                 | TEXT      | Preferred language               |
| mobile                   | TEXT      | Mobile number                    |
| phone                    | TEXT      | Phone number                     |
| time_zone                | TEXT      | Time zone                        |
| available                | BOOLEAN   | Availability flag                |
| deactivated              | BOOLEAN   | From source                      |
| status                   | TEXT      | Derived: active/inactive         |
| focus_mode               | BOOLEAN   | Focus mode                       |
| agent_operational_status | TEXT      | Operational status               |
| last_active_at           | TIMESTAMP | Last active time                 |
| created_at               | TIMESTAMP | Created time                     |
| updated_at               | TIMESTAMP | Updated time                     |
| sync_day                 | TEXT      | day1/day2                        |
| source                   | TEXT      | api                              |
| ingested_at              | TIMESTAMP | Load time                        |


#### `agents` (Final)

Same structure as `stg_agents`, but represents **latest state only**.


#### `agent_status_history`

| Column    | Type    | Description   |
|-----------|---------|---------------|
| agent_id  | BIGINT  | Agent ID      |
| sync_date | DATE    | Snapshot date |
| is_active | BOOLEAN | Active status |


#### `agent_pipeline_errors`

| Column        | Type      | Description        |
|---------------|-----------|--------------------|
| id            | SERIAL    | PK                 |
| agent_id      | BIGINT    | Agent ID           |
| error_type    | TEXT      | VALIDATION_ERROR   |
| error_message | TEXT      | Error description  |
| raw_record    | JSONB     | Full raw row       |
| created_at    | TIMESTAMP | Error time         |


## Execution Order

Step 1 – Activate Virtual Environment

```
source venv/bin/activate
```

Step 2 – Run Spark Pipeline (Loads Staging)
```
Day 1

python -m src.pipelines.agents.agents_pipeline --day day1

Day 2

python -m src.pipelines.agents.agents_pipeline --day day2
```
Step 3 – Run SQL Upserts + History Snapshot

```
psql -d rithvik_zluri_pipeline_db -f src/db/migrations/010_upsert_agents.sql
```