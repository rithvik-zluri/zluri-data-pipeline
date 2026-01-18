# Prefect Orchestration – Zluri Data Pipeline

## 1. Overview

This project uses **Prefect** as the orchestration layer for the Zluri data pipeline. Prefect is responsible for:

* Coordinating execution of multiple data ingestion and transformation steps
* Enforcing execution order and dependencies
* Capturing observability signals (success/failure, retries)
* Passing runtime context (day, flow run id) into SQL for metrics

The pipeline follows a **bronze → silver** style architecture:

* **Staging (stg_*)** tables are loaded via Spark/Python
* **Core tables** are populated via SQL upserts
* **Prefect** orchestrates and tracks the entire workflow

---

## 2. High-Level Architecture

```
+------------------+
|  Prefect Flow    |
|  (zluri_day)     |
+--------+---------+
         |
         v
+------------------+
| Spark Pipelines  |
| (staging load)   |
+--------+---------+
         |
         v
+------------------+
| SQL Upserts      |
| (Postgres)       |
+--------+---------+
         |
         v
+------------------+
| Metrics & State  |
| (pipeline tables)|
+------------------+
```

---

## 3. Prefect Flow

### File

`src/orchestration/prefect_flow.py`

### Purpose

The Prefect **flow** defines the top-level orchestration for a single pipeline run (usually parameterized by `day`).

### Responsibilities

* Accept runtime parameters (e.g. `day=day1`)
* Trigger Spark-based ingestion tasks
* Trigger SQL upsert tasks in correct order
* Enforce dependencies between tasks

### Conceptual Flow

```python
@flow(name="zluri_day_pipeline")
def zluri_day_pipeline(day: str):
    agents = agents_task(day)
    groups = groups_task(day, wait_for=[agents])
    transactions = transactions_task(day, wait_for=[groups])
```

### Key Design Choices

* **Explicit ordering** using `wait_for`
* **Idempotent runs** – safe to rerun for the same day
* **Single-day isolation** – each run is scoped to one logical day

---

## 4. Prefect Tasks

### File

`src/orchestration/prefect_tasks.py`

Each task represents a **logical unit of work** and maps closely to one SQL migration or ingestion step.

### Example Tasks

| Task Name           | Responsibility                             |
| ------------------- | ------------------------------------------ |
| `agents_task`       | Upsert agents + memberships                |
| `groups_task`       | Upsert groups, memberships, inactive logic |
| `transactions_task` | Deduped upsert of transactions             |

### Characteristics

* Thin wrappers around SQL execution
* All heavy logic lives in SQL
* Tasks are deterministic and repeatable

---

## 5. SQL Execution Layer

### File

`src/db/connection.py`

### Function

`run_sql_file(sql_file: str, day: str)`

### Responsibilities

* Open SQL file
* Inject Prefect runtime context into Postgres session
* Execute SQL in a single transaction

### Runtime Context Injection

Before executing SQL, Prefect sets session variables:

```sql
SET app.flow_run_id = '<prefect-flow-run-id>';
SET app.day = '<day>';
```

These values are later read inside SQL using:

```sql
current_setting('app.flow_run_id', true)
current_setting('app.day', true)
```

This enables **end-to-end lineage and metrics**.

---

## 6. SQL Upsert Design Pattern

All SQL migrations follow a consistent structure:

1. Capture execution context
2. Perform upserts / deletes / recomputations
3. Collect metrics using CTEs
4. Write a single row into `pipeline_metrics`

### Template Pattern

```sql
BEGIN;

WITH context AS (...),
     work_step_1 AS (...),
     work_step_2 AS (...),
     metrics AS (...),
     finished AS (...)

INSERT INTO pipeline_metrics (...)
SELECT ...;

COMMIT;
```

This ensures:

* Atomic execution
* No partial writes
* Metrics are written **only if the transaction succeeds**

---

## 7. Groups Pipeline (Deep Dive)

### SQL File

`020_upsert_groups.sql`

### Responsibilities

1. Upsert groups from `stg_groups`
2. Refresh group memberships
3. Compute recursive inactive state
4. Emit pipeline metrics

### Recursive Logic

A recursive CTE (`WITH RECURSIVE`) builds a full group hierarchy:

* Each group is treated as a root
* Parent relationships are traversed upward
* Any group with an **active agent in its subtree** is marked active

This guarantees:

* Correct propagation of active/inactive state
* No reliance on application-side recursion

---

## 8. Transactions Pipeline (Deep Dive)

### SQL File

`040_upsert_transactions.sql`

### Key Features

* **Deduplication** using `DISTINCT ON (transaction_id)`
* **Last-write-wins** using `updated_time`
* **Idempotent upserts** via `ON CONFLICT`
* **Incremental processing** tracked via `transaction_pipeline_state`

### Safety Guarantees

* Older updates never overwrite newer data
* Re-running the same day produces no duplicates
* Late-arriving updates are handled correctly

---

## 9. Pipeline Metrics

### Table

`pipeline_metrics`

### Purpose

Central observability table for the entire pipeline.

### Captured Fields

| Column             | Description                           |
| ------------------ | ------------------------------------- |
| `flow_run_id`      | Prefect flow run identifier           |
| `task_name`        | Logical task name                     |
| `pipeline_name`    | Domain (groups, agents, transactions) |
| `table_name`       | Target table                          |
| `day`              | Logical day                           |
| `rows_inserted`    | New records                           |
| `rows_updated`     | Updated records                       |
| `rows_deleted`     | Deleted records                       |
| `duration_seconds` | End-to-end runtime                    |
| `status`           | success / failure                     |

### Why This Matters

* Enables debugging without Prefect UI
* Provides auditability
* Allows downstream SLA tracking

---


