# Transactions Pipeline Documentation

## Overview

The **Transactions Pipeline** is responsible for:

- Ingesting transaction data from **Zluri/Freshchat source files**
- Transforming it into the standardized **Transaction entity**
- Converting all amounts to **USD**
- Loading the processed data into **PostgreSQL**

It ensures:

- **Incremental processing**
- **Currency normalization**
- **Deduplication**
- **Idempotent loads**

The pipeline follows the pattern:

**staging → validation → currency conversion → deduplication → idempotent load → upsert → mapping update**


This design guarantees **correctness, traceability, and safe re-runs**.

---

## Design Principles

The pipeline is built to be:

- **Incremental** – processes only new or updated transactions  
- **Idempotent** – safe to re-run for the same sync day  
- **Validated** – invalid records are captured in an error table  
- **Deduplicated** – no duplicate transactions are allowed  
- **Currency-aware** – converts all amounts to USD using historical rates  
- **Audit-friendly** – stores both original and converted values  

---

## High-Level Flow
```
API JSON (transactions folder)
 ↓
read_transactions()
 ↓
transform_transactions()
 ├── schema parsing
 ├── currency conversion (API + fallback)
 ├── validation
 ├── idempotency key generation
 ├── deduplication (unique key based)
 ↓
stg_transactions (staging table)  +  transaction_pipeline_errors
 ↓
040_upsert_transactions.sql
 ↓
transactions (final table)
```


---

## Table Definitions

### stg_transactions (Staging)

| Column             | Type      | Description                     |
|--------------------|-----------|---------------------------------|
| transaction_id     | TEXT      | Transaction ID from source      |
| transaction_uuid   | TEXT      | UUID from source                |
| occurred_time      | TIMESTAMP | When transaction occurred       |
| updated_time       | TIMESTAMP | Last update time                |
| user_id            | TEXT      | User ID                         |
| user_uuid          | TEXT      | User UUID                       |
| user_name          | TEXT      | User name                       |
| merchant_name      | TEXT      | Normalized merchant name        |
| raw_merchant_name  | TEXT      | Raw merchant name               |
| card_id            | TEXT      | Card ID                         |
| card_uuid          | TEXT      | Card UUID                       |
| budget_id          | TEXT      | Budget ID                       |
| budget_uuid        | TEXT      | Budget UUID                     |
| original_amount    | NUMERIC   | Original amount                 |
| original_currency  | TEXT      | Original currency code          |
| amount_usd         | NUMERIC   | Converted USD amount            |
| exchange_rate      | NUMERIC   | FX rate used                    |
| fx_source          | TEXT      | FX extracted from               |
| raw_payload        | JSONB     | Full raw transaction record     |
| ingested_at        | TIMESTAMP | Load time                       |

---

### transactions (Final)

| Column             | Type      | Description                     |
|--------------------|-----------|---------------------------------|
| transaction_id     | TEXT      | Primary Key                     |
| transaction_uuid   | TEXT      | UUID from source                |
| occurred_time      | TIMESTAMP | When transaction occurred       |
| updated_time       | TIMESTAMP | Last update time                |
| user_id            | TEXT      | User ID                         |
| user_uuid          | TEXT      | User UUID                       |
| user_name          | TEXT      | User name                       |
| merchant_name      | TEXT      | Normalized merchant name        |
| raw_merchant_name  | TEXT      | Raw merchant name               |
| card_id            | TEXT      | Card ID                         |
| card_uuid          | TEXT      | Card UUID                       |
| budget_id          | TEXT      | Budget ID                       |
| budget_uuid        | TEXT      | Budget UUID                     |
| original_amount    | NUMERIC   | Original amount                 |
| original_currency  | TEXT      | Original currency               |
| amount_usd         | NUMERIC   | USD amount                      |
| exchange_rate      | NUMERIC   | FX rate used                    |
| fx_source          | TEXT      | FX extracted from               |
| idempotency_key    | TEXT      | Unique hash for deduplication   |
| created_at         | TIMESTAMP | Created time                    |
| updated_at         | TIMESTAMP | Updated time                    |
| ingested_at        | TIMESTAMP | Load time                       |

**Primary Key:** transaction_id  
**Unique Constraint:** idempotency_key  

---

### transaction_pipeline_state

| Column              | Type      | Description                |
|---------------------|-----------|----------------------------|
| pipeline_name       | TEXT      | Pipeline identifier        |
| last_processed_time | TIMESTAMP | Latest processed timestamp |

---

### transaction_pipeline_errors

| Column         | Type      | Description         |
|----------------|-----------|---------------------|
| id             | SERIAL    | Primary key         |
| transaction_id | TEXT      | Transaction ID      |
| error_type     | TEXT      | VALIDATION_ERROR    |
| error_message  | TEXT      | Error description   |
| raw_record     | JSONB     | Full raw row        |
| created_at     | TIMESTAMP | Error time          |

---

## Why We Use Staging Tables

- Idempotency – safe re-runs without duplication  
- Debuggability – easy inspection via `SELECT * FROM stg_transactions`  
- Separation of concerns – Spark handles logic, SQL handles persistence  

---

## Why We Use INSERT ... ON CONFLICT (Upsert)

Day 1 → INSERT transactions  
Day 2 → UPDATE existing transactions  
Day 3 → UPDATE again  

```sql
INSERT INTO transactions (...)
SELECT ... FROM stg_transactions
ON CONFLICT (transaction_id)
DO UPDATE SET ...
```

## Result

- **Incremental**
- **Idempotent**
- **Safe for re-runs**
- **Always reflects the latest state**

---

## Currency Conversion Logic

Transactions may arrive in different currencies (INR, EUR, GBP, etc.).

### Strategy

- **Primary:** ExchangeRate API (historical endpoint)  
- **Fallback:** `exchangeRate` field from JSON payload  

### Important Details

- Conversion is based on **transaction date**, not processing date  
- API provides: `1 USD = X currency`  
- We compute: `currency → USD = 1 / X`  
- In-memory cache prevents repeated API calls per date  

### Stored Fields

- `original_amount`
- `original_currency`
- `exchange_rate`
- `amount_usd`

This ensures **full auditability**.

---

## Deduplication Logic

Each transaction is assigned a deterministic **idempotency key**:

```
sha256(
    transaction_id ||
    transaction_uuid ||
    occurred_time ||
    original_amount ||
    original_currency
)
```

## Why

- Prevents duplicate records  
- Ensures safe re-runs  
- Enables idempotent processing  

Duplicates are dropped within Spark before loading to staging.

---

## Validation Rules

A record is considered **invalid** if:

- `transaction_id` is null  
- `transaction_uuid` is null  
- `occurred_time` is null  
- `original_currency` is null  
- `original_amount` is null  
- `exchange_rate` is missing  

Invalid records are written to:

```
transaction_pipeline_errors
```
Pipeline continues processing valid records.

## Incremental & Idempotency Guarantees

- `transactions` table upserts on `transaction_id`  
- `idempotency_key` prevents duplicate logical records  

### Ensures

- New records are inserted  
- Updated records overwrite older versions  
- Older data never overwrites newer data  
- Re-running the same day will not create duplicates  

---

---

## Pipeline State Tracking

After each run, the pipeline updates:

```
transaction_pipeline_state.last_processed_time
```
Using:
```
SELECT MAX(updated_time) FROM stg_transactions
```

#### Ensures

- Only new/updated records are processed in future  
- Full support for incremental loads  

---

## Execution Order

### Step 1 – Activate venv

```bash
source venv/bin/activate
```

### Step 2 – Run Spark Pipeline
```
python -m src.pipelines.transactions.transactions_pipeline --day day1

python -m src.pipelines.transactions.transactions_pipeline --day day2
```

### Step 3 – Run SQL Upsert
```
psql -d rithvik_zluri_pipeline_db -f src/db/migrations/040_upsert_transactions.sql
```