# Cards Pipeline Documentation

## Overview

The **Cards Pipeline** is responsible for ingesting, validating, transforming, and loading card data from the Zluri API JSON responses into PostgreSQL.

It follows a:

**staging → validation → deduplication → idempotent load → upsert**

pattern to ensure correctness, traceability, and safe re-runs.

This pipeline is designed to be:

- **Idempotent** – safe to re-run for the same sync day  
- **Validated** – invalid records are captured in an error table  
- **Deduplicated** – within-batch duplicates are removed  
- **Upsert-driven** – final tables always reflect the latest state  
- **Transaction-ready** – designed to support downstream transactions pipeline

---

## High-Level Flow

```text
API JSON (cards folder)
 ↓
read_cards_raw()
 ↓
transform_cards()
 ├── validation
 ├── deduplication
 ↓
stg_cards (staging table)
 ↓
031_upsert_cards.sql
 ↓
cards (final table)
```

## Table Definitions

### `stg_cards` (Staging)

| Column              | Type      | Description                    |
|---------------------|-----------|--------------------------------|
| card_id             | TEXT      | Card ID from source            |
| card_uuid           | TEXT      | Card UUID                      |
| name                | TEXT      | Card name                      |
| user_id             | TEXT      | User ID                        |
| user_uuid           | TEXT      | User UUID                      |
| budget_id           | TEXT      | Budget ID                      |
| budget_uuid         | TEXT      | Budget UUID                    |
| last_four           | TEXT      | Last 4 digits of card          |
| valid_thru          | TEXT      | Expiry                         |
| status              | TEXT      | Card status                    |
| type                | TEXT      | Card type                      |
| share_budget_funds  | BOOLEAN   | Shared budget flag             |
| recurring           | BOOLEAN   | Recurring flag                 |
| recurring_limit     | NUMERIC   | Recurring limit                |
| current_limit       | NUMERIC   | Current period limit           |
| current_spent       | NUMERIC   | Current period spent           |
| created_time        | TIMESTAMP | Created time                   |
| updated_time        | TIMESTAMP | Updated time                   |
| sync_day             | TEXT      | day1 / day2                    |
| source               | TEXT      | api                            |
| ingested_at          | TIMESTAMP | Load time                      |

---

### `cards` (Final)

Same structure as `stg_cards` plus:

| Column           | Type |
|------------------|------|
| last_synced_day  | TEXT |

Represents the **latest state of each card**.

---

### `card_pipeline_errors`

| Column        | Type      | Description        |
|--------------|-----------|--------------------|
| id           | SERIAL    | PK                 |
| card_id      | TEXT      | Card ID            |
| error_type   | TEXT      | VALIDATION_ERROR   |
| error_message| TEXT      | Description        |
| raw_record   | JSONB     | Full raw row       |
| sync_day     | TEXT      | day1 / day2        |
| created_at  | TIMESTAMP | Error time         |

## Validation Rules

A card record is considered **invalid** if:

- `card_id` is `NULL`  
- `budget_id` is `NULL`  
- `status` is `NULL`  
- `type` is `NULL`  
- `created_time` is `NULL`  
- `current_limit < 0`  
- `current_spent < 0`  
- `recurring_limit < 0`  

Invalid records are written to:

- `card_pipeline_errors`

The pipeline **continues processing valid records**.

---

## Deduplication

Within each batch:

```
valid_df = valid_df.dropDuplicates(["card_id"])
```

This ensures:

- No duplicate card records in staging  
- Clean upsert behavior  

---

## Why We Use Staging (`stg_cards`)

Staging tables give:

- **Idempotency** – safe re-runs  
- **Debuggability** – `SELECT * FROM stg_cards`  
- **Separation of concerns** – Spark = logic, SQL = persistence  

This is **industry-standard ETL design**.

---

## Why We Use `INSERT ... ON CONFLICT` (Upsert)

### Requirement

We need:

- Day 1 → **INSERT**  
- Day 2 → **UPDATE existing cards**  
- Day 3 → **UPDATE again**  

### Pattern

```
INSERT INTO cards (...)
SELECT ... FROM stg_cards
ON CONFLICT (card_id)
DO UPDATE SET ...
```

## Result

- Incremental  
- Idempotent  
- Safe for re-runs  
- Always reflects **latest state**

---

## Idempotency Handling

Before writing to `stg_cards`, the pipeline:

- Reads existing `stg_cards`  
- Filters out records already loaded for the same `sync_day`  

This ensures:

- Same day re-run does **not duplicate data**  
- Clean **day1 → day2** simulation  

---

## Why Cards Pipeline Is Transaction-Ready

The **transactions pipeline depends on cards**.

Therefore:

- `card_id` is enforced as **NOT NULL**  
- Deduplication is **strict**  
- Status and budget linkage is **validated**  
- Bad records are **isolated early**  

This guarantees:

- Transactions always reference **valid cards**  
- No FK or join issues later  

---

## Execution Order (Correct Way)

Always run in this order 👇

### Step 1 – Activate venv
```
source venv/bin/activate
```

Step 2 – Run Spark Pipeline (loads staging)
```
Day 1

python -m src.pipelines.cards.cards_pipeline --day day1


Day 2

python -m src.pipelines.cards.cards_pipeline --day day2
```

Step 3 – Run SQL Upsert
```
psql -d rithvik_zluri_pipeline_db -f src/db/migrations/031_upsert_cards.sql
```