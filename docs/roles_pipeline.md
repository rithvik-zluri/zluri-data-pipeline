# Roles Pipeline

## Overview

The Roles Pipeline is responsible for ingesting role data from source systems, transforming it into the standardized Role entity, and storing it in the database. It also handles mapping between agents and roles.

This pipeline ensures that:
- Roles are uniquely identified and upserted
- Agent ↔ Role relationships are maintained
- The pipeline is idempotent and safe to re-run

---

## Source Data

- Location: `sample_data/sync-dayX/admin_roles/`
- Format: JSON
- Each record represents a role with associated metadata

---

## Target Schema

### roles table

| Column      | Type      | Description                  |
|-------------|-----------|------------------------------|
| role_id     | BIGINT    | Unique role identifier       |
| name        | TEXT      | Role name                    |
| description | TEXT      | Role description             |
| created_at  | TIMESTAMP | Creation timestamp           |
| updated_at  | TIMESTAMP | Last updated timestamp       |

### agent_role_mapping table

| Column   | Type   | Description              |
|----------|--------|--------------------------|
| agent_id | BIGINT | Reference to agent       |
| role_id  | BIGINT | Reference to role        |

---

## Pipeline Steps

1. **Ingest**
   - Read JSON files from `admin_roles`
   - Load into Spark DataFrame

2. **Schema Validation**
   - Ensure required fields are present
   - Log and skip malformed records

3. **Transform**
   - Select required fields
   - Normalize column names
   - Prepare mapping records for agent-role relationships

4. **Load**
   - Upsert into `roles` table
   - Upsert into `agent_role_mapping` table

---

## Idempotency

- Roles are upserted based on `role_id`
- Agent-role mappings use `(agent_id, role_id)` as composite key
- Re-running the pipeline will not create duplicates

---

## Error Handling

- Invalid records are logged with reason
- Pipeline continues processing valid records

---

## Example Run

```bash
python -m src.pipelines.roles_pipeline --day day1
python -m src.pipelines.roles_pipeline --day day2