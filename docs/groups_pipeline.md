# Groups Pipeline Documentation

## Overview

The **Groups Pipeline** is responsible for ingesting group and group membership data from the Zluri/Freshchat API JSON responses into PostgreSQL.

It handles:
- **Hierarchical group structures (parent → child → sub-child …)**
- **Many-to-many agent ↔ group relationships**
- **Inactive group detection based on agent activity across the hierarchy**

It follows a:

**staging → validation → deduplication → idempotent load → upsert → hierarchical inactive computation**

pattern to ensure correctness, traceability, and safe re-runs.

This pipeline is designed to be:

- **Idempotent** – safe to re-run for the same sync day  
- **Hierarchy-aware** – supports arbitrary depth group nesting  
- **Validated** – invalid records are captured in an error table  
- **Deduplicated** – within-batch duplicates are removed  
- **Upsert-driven** – final tables always reflect the latest state  
- **State-aware** – correctly computes inactive groups across hierarchy  

---

## High-Level Flow

```text
API JSON (admin_groups folder)
 ↓
read_groups_raw()
 ↓
transform_groups()
 ├── validation
 ├── hierarchy normalization
 ├── membership explosion
 ├── deduplication
 ↓
stg_groups, stg_group_membership (staging tables)
 ↓
020_upsert_groups.sql
 ↓
groups, group_membership (final tables)
 ↓
inactive computation (hierarchical)
```

### Why We Support Hierarchy (Parent → Child Groups)


A group is considered active if:

- It has at least one active agent directly
OR

- Any of its child groups (at any depth) has an active agent

Without hierarchy handling:

- Parent groups would be incorrectly marked inactive

- Reporting and routing logic would break

This is true state modeling, not flat data.

### Why Inactive Is Computed Using Hierarchy

A group is inactive if:

- It has no active agents directly and


- None of its descendant groups (children, grandchildren, etc.) have active agents


### Why We Write to Staging First (stg_groups, stg_group_membership)

Staging tables provide:

- Idempotency – safe re-runs

- Debuggability – SELECT * FROM stg_groups

- Separation of concerns – Spark = transform, SQL = persistence


### Why We Use INSERT ... ON CONFLICT (Upsert)

We need:

Day 1 → INSERT

Day 2 → UPDATE existing rows

Day 3 → UPDATE again

Pattern
```
INSERT INTO groups (...)
SELECT ... FROM stg_groups
ON CONFLICT (group_id)
DO UPDATE SET ...
```
## Result

- Incremental

- Idempotent

- Safe for re-runs

- Always reflects latest state

### Why We Separate groups and group_membership


They represent two different relationships:

- groups → entity metadata (name, hierarchy, inactive)

- group_membership → many-to-many mapping (agent ↔ group)

This avoids:

- Data duplication

- Update anomalies

- Broken normalization

## Table Definitions

### `stg_groups` (Staging)

| Column          | Type      | Description          |
|-----------------|-----------|----------------------|
| group_id        | BIGINT    | Group ID from source |
| name            | TEXT      | Group name           |
| parent_group_id | BIGINT    | Parent group ID      |
| created_at      | TIMESTAMP | Created time         |
| updated_at      | TIMESTAMP | Updated time         |

### `stg_group_membership` (Staging)

| Column   | Type   | Description |
|----------|--------|-------------|
| group_id | BIGINT | Group ID    |
| agent_id | BIGINT | Agent ID    |

### `groups`

| Column          | Type      | Description           |
|-----------------|-----------|-----------------------|
| group_id        | BIGINT    | Group ID              |
| name            | TEXT      | Group name            |
| parent_group_id | BIGINT    | Parent group ID       |
| created_at      | TIMESTAMP | Created time          |
| updated_at      | TIMESTAMP | Updated time          |
| inactive        | BOOLEAN   | Derived inactive flag |

### `group_membership` (Final)

| Column   | Type   | Description |
|----------|--------|-------------|
| group_id | BIGINT | Group ID    |
| agent_id | BIGINT | Agent ID    |

**Primary Key:** `(group_id, agent_id)`

### `group_pipeline_errors`

| Column        | Type      | Description       |
|---------------|-----------|-------------------|
| id            | SERIAL    | PK                |
| group_id      | BIGINT    | Group ID          |
| error_type    | TEXT      | VALIDATION_ERROR  |
| error_message | TEXT      | Error description |
| raw_record    | JSONB     | Full raw row      |
| created_at    | TIMESTAMP | Error time        |


## Execution Order


Step 1 – Activate venv

```
source venv/bin/activate
```

Step 2 – Run Spark Pipeline (loads staging)

```
Day 1

python -m src.pipelines.groups.groups_pipeline --day day1


Day 2

python -m src.pipelines.groups.groups_pipeline --day day2
```

Step 3 – Run SQL Upserts

```
psql -d rithvik_zluri_pipeline_db -f src/db/migrations/020_upsert_groups.sql
```