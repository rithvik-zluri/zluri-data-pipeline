# test_data (edge-case dataset)

This folder contains **intentionally tricky/invalid** records in the **same directory layout** as `sample_data/`, so you can point peers to it and quickly surface bugs in their pipelines.

## How to run against this data

Set:

- `DATA_BASE_PATH=test_data`
- Run your flows with `--day test1` (because this folder uses `sync-test1/`).

Example:

```bash
export DATA_BASE_PATH=test_data
python -m src.orchestration.prefect_flow --day test1
```

## What’s inside

All data lives under `test_data/sync-test1/`.

### agents (`agents/1.json`)

- **A1 valid**: one normal agent.
- **A2 missing id**: `id: null` (should be dropped/errored).
- **A3 wrong types**: `id` as string, `ticket_scope` as string, `created_at` invalid timestamp.
- **A4 schema drift**: `contact` is `null` (pipelines that assume nested fields exist may crash).

### agent_details (`agent_details/*.json`)

- **AD1 valid**: one normal agent_details record.
- **AD2 role_ids wrong type**: `role_ids` is a string instead of array.
- **AD3 null timestamps**: `created_at` / `updated_at` are null.

### roles (`roles/1.json`)

- **R1 duplicate ids**: same `id` appears twice (tests dedup logic).
- **R2 invalid row**: `id: null`, `agent_type` wrong type.

### admin_groups (`admin_groups/1.json`)

- **G1/G2 cycle**: `parent_group_id` creates a cycle (1 ↔ 2). Any recursive hierarchy builder should handle this safely.
- **G3 agent_ids wrong type**: `agent_ids` is a string (tests schema drift tolerance).
- **G4 invalid agent id**: `agent_ids` includes an id that does not exist in agents.
- **G5 missing group id**: `id: null`.

### budgets (`budgets/1.json`)

- **B1 missing id**: `id: null`.
- **B2 missing limit**: `currentPeriod.limit: null` (should land in error table).
- **B3 wrong type**: `currentPeriod.limit` as string.
- **B4 negative limit**: `currentPeriod.limit: -10`.

### cards (`cards/1.json`)

- **C1 wrong boolean types**: `shareBudgetFunds: "true"` and `recurring: "false"` (string booleans).
- **C2 missing createdTime**: `createdTime: null` (should be invalid).
- **C3 missing status/type**: `status: null`, `type: null`.
- **C4 negative numbers**: `currentPeriod.limit < 0`.

### transactions (`transactions/1.json`)

- **T1 wrong amount type**: `amount: "1497.22"` (string).
- **T2 invalid occurredTime**: malformed timestamp.
- **T3 currencyData wrong types**: `originalCurrencyAmount` non-numeric.
- **T4 missing required tag values**: required tags empty (tests validation / error routing).

