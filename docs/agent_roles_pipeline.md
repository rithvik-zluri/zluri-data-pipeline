# Agent Roles Pipeline Documentation

## Overview

The **Agent Roles Pipeline** is responsible for ingesting, transforming, and maintaining **agent ↔ role assignments** from the Zluri/Freshchat API into PostgreSQL.

This pipeline ensures that:
- Agent ↔ Role relationships are always accurate
- Role assignments and removals are tracked correctly
- The latest state is reflected in the mapping table
- History is preserved through status tracking

It follows a:

**staging → validation → deduplication → idempotent load → upsert → relationship management**

pattern to ensure correctness, traceability, and safe re-runs.

This pipeline is designed to be:

- **Idempotent** – safe to re-run for the same sync day  
- **Validated** – invalid records are captured in an error table  
- **Deduplicated** – within-batch duplicates are removed  
- **Upsert-driven** – relationships are updated incrementally  
- **Relationship-aware** – handles both assignment and removal cleanly  

---

## High-Level Flow

```text
API JSON (admin_roles folder)
 ↓
read_roles_raw()
 ↓
transform_agent_roles()
 ├── validation
 ├── deduplication
 ├── assignment / removal detection
 ↓
stg_agent_roles (staging table)
 ↓
012_upsert_agent_roles.sql
 ↓
agent_role_mapping (final table)
```
## Why Agent Roles Is a Separate Pipeline

The **Roles Pipeline** handles:

- Role entities (`role_id`, `name`, `description`, etc.)

The **Agent Roles Pipeline** handles:

- Relationships between agents and roles  
- Assignment and removal tracking  
- Role state changes over time  

### Why This Separation Exists

Separating these pipelines gives:

- **Clear responsibility boundaries**  
- **Simpler transformation logic**  
- **Easier debugging and traceability**  
- **Correct state management over time**  

This follows **industry-standard dimensional modeling** where:

- **Dimensions** (roles) are managed independently  
- **Mappings / relationships** (agent ↔ role) are handled in dedicated pipelines  

This design prevents:
- Data duplication  
- Update anomalies  
- Broken history tracking  

And ensures:
- Clean schema design  
- Accurate auditing  
- Production-grade maintainability  

## Table Definitions

### `stg_agent_roles` (Staging)

| Column       | Type      | Description             |
|-------------|-----------|-------------------------|
| agent_id    | BIGINT    | Agent ID                |
| role_id     | BIGINT    | Role ID                 |
| status      | TEXT      | assigned / removed      |
| sync_day    | TEXT      | day1 / day2             |
| source      | TEXT      | api                     |
| ingested_at | TIMESTAMP | Load time               |

---

### `agent_role_mapping` (Final)

| Column       | Type      | Description             |
|-------------|-----------|-------------------------|
| agent_id    | BIGINT    | Agent ID                |
| role_id     | BIGINT    | Role ID                 |
| status      | TEXT      | assigned / removed      |
| assigned_at | TIMESTAMP | Assigned timestamp      |
| removed_at  | TIMESTAMP | Removed timestamp       |

**Composite Key:** `(agent_id, role_id)`

---

### `agent_role_pipeline_errors`

| Column        | Type      | Description        |
|--------------|-----------|--------------------|
| id           | SERIAL    | PK                 |
| agent_id     | BIGINT    | Agent ID           |
| role_id      | BIGINT    | Role ID            |
| error_type   | TEXT      | VALIDATION_ERROR   |
| error_message| TEXT      | Description        |
| raw_record   | JSONB     | Full raw row       |
| created_at   | TIMESTAMP | Error time         |

## Why We Use Staging (`stg_agent_roles`)

### Reasoning

Staging tables give:

- **Idempotency** – safe re-runs  
- **Debuggability** – `SELECT * FROM stg_agent_roles`  
- **Separation of concerns** – Spark = logic, SQL = persistence  

This is **industry-standard ETL design**.

---

## Assignment & Removal Logic

Each role record contains assigned agents.

From this we derive:

- **assigned** → agent present in role  
- **removed** → agent no longer present in role  

### Logic

- If agent appears in role → `status = 'assigned'`  
- If agent missing from role but exists previously → `status = 'removed'`  

This ensures:

- Clean handling of role removals  
- Accurate state reflection  
- No stale mappings  

---

## Why We Calculate This in Spark (Not SQL)

### Reasoning

- Avoids complex `NOT IN` / `EXCEPT` queries  
- Keeps SQL simple  
- Business logic stays in transformation layer  
- Correct **day1 → day2 → day3** transitions  

This is the **correct design pattern**.

---

## Why We Use `INSERT ... ON CONFLICT` (Upsert)

### Requirement

We need:

- Day 1 → **INSERT**  
- Day 2 → **UPDATE existing rows**  
- Day 3 → **UPDATE again**  

### Pattern

```sql
INSERT INTO agent_role_mapping (...)
SELECT ... FROM stg_agent_roles
ON CONFLICT (agent_id, role_id)
DO UPDATE SET ...
```

## Result

- **Incremental**  
- **Idempotent**  
- **Safe for re-runs**  
- **Always reflects latest state**

---

## Validation Rules

A record is considered **invalid** if:

- `agent_id` is `NULL`  
- `role_id` is `NULL`  
- `status` is `NULL`  

Invalid records are written to:

- `agent_role_pipeline_errors`

The pipeline **continues processing valid records**.

---

## Idempotency Guarantees

- Composite key `(agent_id, role_id)` prevents duplicates  
- Duplicate mappings within batch are dropped  
- Re-running same day will not create duplicates  
- Final table always reflects **latest assignment state**

---

## Execution Order


Step 1 – Activate venv
```
source venv/bin/activate
```
Step 2 – Run Spark Pipeline (loads staging)
```
Day 1

python -m src.pipelines.agent_roles.agent_roles_pipeline --day day1

Day 2

python -m src.pipelines.agent_roles.agent_roles_pipeline --day day2
```

Step 3 – Run SQL Upsert
```
psql -d rithvik_zluri_pipeline_db -f src/db/migrations/012_upsert_agent_roles.sql
```