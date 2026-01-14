# Budgets Pipeline Documentation
## Overview

The **Budgets Pipeline** is responsible for ingesting, validating, transforming, and loading budget data from the Zluri API JSON responses into PostgreSQL.  
It follows a **staging → validation → deduplication → idempotent load → upsert** pattern to ensure data quality and reliability.

This pipeline is designed to be:
- **Idempotent** – safe to re-run for the same sync day
- **Validated** – invalid records are captured in an error table
- **Deduplicated** – within-batch duplicates are removed
- **Upsert-driven** – final tables always reflect the latest state

---

## High-Level Flow

```text
API JSON
   ↓
read_budgets_raw()
   ↓
transform_budgets()
   ├── validation
   ├── deduplication
   ↓
stg_budgets (staging table)
   ↓
031_upsert_budgets.sql
   ↓
budgets (final table)
```
---

## Table Definitions

#### `stg_budgets` (Staging)

| Column | Type | Description |
|--------|------|-------------|
| budget_id | TEXT | Budget ID from source |
| budget_uuid | TEXT | Budget UUID |
| name | TEXT | Budget name |
| description | TEXT | Budget description |
| retired | BOOLEAN | Retired flag |
| start_date | DATE | Budget start date |
| recurring_interval | TEXT | Recurrence |
| timezone | TEXT | Timezone |
| limit_amount | NUMERIC | Budget limit |
| overspend_buffer | NUMERIC | Overspend buffer |
| assigned_amount | NUMERIC | Assigned amount |
| spent_cleared | NUMERIC | Cleared spend |
| spent_pending | NUMERIC | Pending spend |
| spent_total | NUMERIC | Total spend |
| sync_day | TEXT | day1/day2 |
| source | TEXT | api |
| ingested_at | TIMESTAMP | Load time |

---

### `budgets` (Final)

Same structure as `stg_budgets` plus:

| Column | Type |
|--------|------|
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |

---

### `budget_pipeline_errors`

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | PK |
| budget_id | TEXT | Budget ID |
| error_type | TEXT | VALIDATION_ERROR |
| error_message | TEXT | Description |
| raw_record | JSONB | Full raw row |
| sync_day | TEXT | day1/day2 |
| created_at | TIMESTAMP | Error time |

---

This pipeline ensures:

New budgets → INSERT

Existing budgets → UPDATE

Final table always reflects latest state

Execution Order (Correct Way)
For each sync day:

1. run_budgets_pipeline("day1")
2. run 030_upsert_budgets.sql

3. run_budgets_pipeline("day2")
4. run 030_upsert_budgets.sql