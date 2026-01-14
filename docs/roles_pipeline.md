# Roles Pipeline Documentation

## Overview

The **Roles Pipeline** is responsible for ingesting role data from the Zluri/Freshchat API, transforming it into the standardized Role entity, and loading it into PostgreSQL.  
It also manages **agent ↔ role relationships** and ensures correct assignment tracking.

It follows a:

**staging → validation → deduplication → idempotent load → upsert → mapping update**

pattern to ensure correctness, traceability, and safe re-runs.

This pipeline is designed to be:

- **Idempotent** – safe to re-run for the same sync day  
- **Validated** – invalid records are captured in an error table  
- **Deduplicated** – within-batch duplicates are removed  
- **Upsert-driven** – roles are updated incrementally  
- **Relationship-aware** – maintains agent ↔ role mappings  

---

## High-Level Flow

```text
API JSON (admin_roles folder)
 ↓
read_roles_raw()
 ↓
transform_roles()
 ├── validation
 ├── deduplication
 ├── mapping extraction
 ↓
stg_roles  +  stg_agent_roles (staging tables)
 ↓
011_upsert_roles.sql
 ↓
roles (final table)
 ↓
agent_role_mapping (final table)
```
## Table Definitions

### `stg_roles` (Staging)

| Column      | Type      | Description        |
|-------------|-----------|--------------------|
| role_id     | BIGINT    | Role ID from source|
| name        | TEXT      | Role name          |
| description | TEXT      | Role description   |
| is_default  | BOOLEAN   | Default role flag  |
| agent_type  | INT       | Agent type         |
| created_at  | TIMESTAMP | Created time       |
| updated_at  | TIMESTAMP | Updated time       |
| sync_day    | TEXT      | day1/day2          |
| source      | TEXT      | api                |
| ingested_at | TIMESTAMP | Load time          |

### `stg_agent_roles` (Staging Mapping)

| Column     | Type      | Description         |
|------------|-----------|---------------------|
| agent_id   | BIGINT    | Agent ID            |
| role_id    | BIGINT    | Role ID             |
| status     | TEXT      | assigned / removed  |
| updated_at | TIMESTAMP | Update time         |

### `roles`

| Column      | Type      | Description      |
|-------------|-----------|------------------|
| role_id     | BIGINT    | Primary Key      |
| name        | TEXT      | Role name        |
| description | TEXT      | Role description |
| is_default  | BOOLEAN   | Default role     |
| agent_type  | INT       | Agent type       |
| created_at  | TIMESTAMP | Created time     |
| updated_at  | TIMESTAMP | Updated time     |

### `agent_role_mapping`

| Column      | Type      | Description        |
|-------------|-----------|--------------------|
| agent_id    | BIGINT    | Agent ID           |
| role_id     | BIGINT    | Role ID            |
| status      | TEXT      | assigned / removed |
| assigned_at | TIMESTAMP | Assigned time      |
| removed_at  | TIMESTAMP | Removed time       |

**Composite Key:** `(agent_id, role_id)`

### `role_pipeline_errors`

| Column        | Type      | Description       |
|---------------|-----------|-------------------|
| id            | SERIAL    | PK                |
| role_id       | BIGINT    | Role ID           |
| error_type    | TEXT      | VALIDATION_ERROR  |
| error_message | TEXT      | Description       |
| raw_record    | JSONB     | Full raw row      |
| created_at    | TIMESTAMP | Error time        |



### Why We Use Staging Tables (stg_roles, stg_agent_roles)

- Idempotency – safe re-runs without duplication

- Debuggability – easy inspection via SELECT * FROM stg_roles

- Separation of concerns – Spark handles logic, SQL handles persistence


### Why We Use INSERT ... ON CONFLICT (Upsert)

Day 1 → INSERT roles

Day 2 → UPDATE existing roles

Day 3 → UPDATE again

### Pattern:

```
INSERT INTO roles (...)
SELECT ... FROM stg_roles
ON CONFLICT (role_id)
DO UPDATE SET ...
```

### Result:

- Incremental

- Idempotent

- Safe for re-runs

- Always reflects latest state

### Agent ↔ Role Mapping Logic

Each role record contains assigned agents

These are extracted into stg_agent_roles

Mapping table uses (agent_id, role_id) composite key

**Status** is tracked as:

- assigned

- removed

This enables:

- Accurate role assignment tracking

- Clean handling of role removals

- Auditable role history

## Validation Rules
A record is considered invalid if:

- role_id is null

- name is null

- agent_id is null in mappings

**Invalid records** are written to:

- role_pipeline_errors

- Pipeline continues processing valid records.
---

## Idempotency Guarantees
- roles table upserts on role_id

- agent_role_mapping uses composite key (agent_id, role_id)

- Duplicate role records within a batch are dropped

- Re-running same day will not create duplicates

Execution Order

Step 1 – Activate venv

```
source venv/bin/activate
```

Step 2 – Run Spark Pipeline (loads staging)
```
Day 1:

python -m src.pipelines.roles.roles_pipeline --day day1
Day 2:

python -m src.pipelines.roles.roles_pipeline --day day2
```
Step 3 – Run SQL Upserts
```
psql -d rithvik_zluri_pipeline_db -f src/db/migrations/011_upsert_roles.sql
```