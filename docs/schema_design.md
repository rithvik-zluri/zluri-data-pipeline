# Schema Design

## Overview

This schema is designed using **PostgreSQL** and follows **normalization principles** to ensure:
- No data duplication
- High data integrity
- Clear relationships between entities
- Scalable analytics and reporting

The schema supports data coming from:
- `sync-day1`
- `sync-day2`
- `tran` (transactional domain)

It is divided into:
1. **Core Entities (Week 1 Scope)**
2. **Mapping & History Tables**
3. **Future / Transactional Entities**

---

## Why PostgreSQL?

PostgreSQL is chosen because:
- Strong relational integrity support
- Excellent performance for joins
- Native support for `JSONB`
- Industry standard for data engineering pipelines

---

## 1. Agents

### Table: `agents`

**Source:**
- `sync-day1/agents`
- `sync-day2/agents`
- `sync-day1/agent_details`
- `sync-day2/agent_details`

**Description:**
Stores core information about support agents such as:
- name
- email
- availability
- operational status
- activation status

**Primary Key:** `agent_id`

Each row represents one unique support agent in the organization.

**Why separate table?**
This is the **central dimension table** used across all pipelines.

---

## 2. Agent Details

### Table: `agent_details`

**Source:**
- `sync-day1/agent_details`
- `sync-day2/agent_details`

**Description:**
Stores extended and nested information such as:
- ticket scope
- signature
- avatar
- last login
- freshchat flags

**Relationship:**
One-to-one with `agents` using `agent_id`.

**Why separate table?**
To keep the main `agents` table clean and avoid excessive nullable columns.

---

## 3. Groups (Admin Groups)

### Table: `groups`

**Source:**
- `sync-day1/admin_groups`
- `sync-day2/admin_groups`

**Description:**
Stores organizational groups like:
- IT Support
- DevOps
- Hardware Procurement

Each group can contain multiple agents.

**Primary Key:** `group_id`

---

## 4. Group Membership (Agent ↔ Group)

### Table: `group_membership`

**Source:**
- `agent_ids` array inside `admin_groups` JSON

**Description:**
Represents the **many-to-many** relationship between agents and groups.

One agent can belong to multiple groups, and one group can contain multiple agents.

**Composite Primary Key:**
- (`group_id`, `agent_id`)

**Why mapping table?**
Relational databases cannot store arrays for relationships efficiently.
This table normalizes the relationship.

---

## 5. Roles

### Table: `roles`

**Source:**
- `sync-day1/roles`
- `sync-day2/roles`

**Description:**
Stores role definitions such as:
- Admin
- Approver
- Requester

Each role defines permissions and responsibilities.

**Primary Key:** `role_id`

---

## 6. Agent ↔ Role Mapping

### Table: `agent_role_mapping`

**Description:**
Stores current role assignments for agents.

Includes:
- assignment status
- assigned_at
- removed_at

---

### Table: `agent_role_history`

**Description:**
Audit table that tracks:
- when roles were assigned
- when roles were removed

Used for:
- compliance
- debugging
- historical analysis

---

## 7. Staging Tables

### Tables:
- `stg_agents`
- `stg_agent_details`
- `stg_groups`
- `stg_group_membership`
- `stg_roles`
- `stg_agent_roles`

**Description:**
These are intermediate tables used by pipelines before loading into final tables.

**Why staging?**
- Safer writes
- Easier debugging
- Supports reprocessing

---

## 8. Error Tables

### Tables:
- `agent_pipeline_errors`
- `group_pipeline_errors`
- `role_pipeline_errors`
- `agent_role_pipeline_errors`

**Description:**
Stores rejected or invalid records with:
- error type
- error message
- raw JSON record

**Why needed?**
- Data quality tracking
- Debugging source issues
- Auditability

---

## 9. Budgets (Future Scope)

### Table: `budgets`

**Source:**
- `sync-day1/budgets`
- `sync-day2/budgets`
- `tran/budgets`

**Description:**
Stores budget allocations and financial limits.

Includes a `source` column to track origin.

---

## 10. Cards (Future Scope)

### Table: `cards`

**Source:**
- `sync-day1/cards`
- `sync-day2/cards`
- `tran/cards`

**Description:**
Stores corporate card details issued to agents.

Linked to:
- `agents`
- `budgets`

---

## 11. Transactions (Future Scope)

### Table: `transactions`

**Source:**
- `sync-day1/transactions`
- `sync-day2/transactions`
- `tran/transactions`

**Description:**
Stores financial transactions made using cards.

Linked to:
- `cards`
- `agents`

---

## 12. Agent Availability (Future Scope)

### Table: `agent_availability`

**Description:**
Tracks real-time availability status per agent and channel.

---

## 13. Agent Status History

### Table: `agent_status_history`

**Description:**
Maintains daily snapshot of agent active/inactive status.

Used for:
- trend analysis
- reporting
- audits

---

## Design Principles

### Normalization
- No duplicate data
- Clear ownership of attributes

### Scalability
- Mapping tables for many-to-many
- History tables for audit

### Traceability
- `source` column
- error tables
- history tables

---

## Summary

This schema is designed to:
- Cleanly represent all provided JSON data
- Support scalable ETL pipelines
- Maintain strong relational integrity
- Enable auditing and historical tracking
- Be future-ready for transactional pipelines
