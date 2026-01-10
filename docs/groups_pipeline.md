# Groups Pipeline

## Overview

The Groups Pipeline is responsible for ingesting group and group membership data, handling hierarchical group structures, and storing them in the database. It also computes inactive status based on agent membership across nested groups.

This pipeline supports:
- Arbitrary depth group hierarchies
- Many-to-many agent ↔ group relationships
- Hierarchical inactive logic

---

## Source Data

- Location: `sample_data/sync-dayX/admin_groups/`
- Format: JSON
- Each record represents a group with optional parent group and agent memberships

---

## Target Schema

### groups table

| Column          | Type      | Description                          |
|-----------------|-----------|--------------------------------------|
| group_id        | BIGINT    | Unique group identifier              |
| name            | TEXT      | Group name                           |
| parent_group_id | BIGINT    | Parent group reference (nullable)    |
| created_at      | TIMESTAMP | Creation timestamp                   |
| updated_at      | TIMESTAMP | Last updated timestamp               |
| inactive        | BOOLEAN   | Inactive status flag                 |

### group_membership table

| Column   | Type   | Description              |
|----------|--------|--------------------------|
| group_id | BIGINT | Reference to group       |
| agent_id | BIGINT | Reference to agent       |

---

## Pipeline Steps

1. **Ingest**
   - Read JSON files from `admin_groups`
   - Load into Spark DataFrame

2. **Schema Validation**
   - Validate presence of required fields
   - Log malformed records

3. **Transform**
   - Extract group attributes
   - Extract and explode agent memberships
   - Normalize hierarchy relationships

4. **Load**
   - Upsert into `groups` table
   - Upsert into `group_membership` table

5. **Inactive Logic**
   - A group is inactive if:
     - It has **no active agents directly**
     - AND **none of its child groups (at any depth)** have active agents
   - Implemented using recursive traversal

---

## Hierarchy Handling

- Supports nested structure: group → subgroup → child group → …
- Uses:
  - Recursive CTEs (Postgres) **or**
  - Tree traversal in Spark before load

---

## Idempotency

- Groups are upserted on `group_id`
- Membership uses `(group_id, agent_id)` composite key
- Safe to re-run without duplicates

---

## Error Handling

- Invalid records logged
- Orphan memberships (agent not found) reported

---

## Example Run

```bash
python -m src.pipelines.groups_pipeline --day day1
python -m src.pipelines.groups_pipeline --day day2