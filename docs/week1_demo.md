# Week 1 Deliverables – Pipeline & Testing Explanation

## Overview

In Week 1, we implemented the core data pipelines and corresponding unit tests using **PySpark** and **pytest**.  
The focus was on:
- Building robust transformation logic
- Handling data quality issues (errors, missing fields)
- Writing clean unit tests for each pipeline

Pipelines covered:
1. Agents Pipeline
2. Roles Pipeline
3. Agent-Roles Pipeline
4. Groups Pipeline

Each pipeline has a corresponding `test_<pipeline>_transform.py` file.

---

## 1. Agents Pipeline

### File
src/pipelines/agents_pipeline.py
tests/test_agents_transform.py


### What the pipeline does
- Reads `agent_details` and `agents` data
- Validates required fields (`id`, `email`, `name`)
- Separates **valid records** and **error records**
- Derives `status` as:
  - `active` → if present in agents list
  - `inactive` → otherwise

### Test Cases Explained

#### 1. `test_valid_active_agent`
**Purpose:**  
Ensure a fully valid agent who exists in the agents list is marked as `active`.

**Checks:**
- `final_df` has 1 record
- `status == "active"`
- `error_df` is empty

---

#### 2. `test_inactive_agent`
**Purpose:**  
Ensure an agent not present in agents list is marked as `inactive`.

**Checks:**
- `status == "inactive"`
- No error record generated

---

#### 3. `test_missing_id_goes_to_error`
**Purpose:**  
Ensure records with missing `id` go to error table.

**Checks:**
- `final_df` is empty
- `error_df` has 1 record
- `error_type == "MISSING_REQUIRED_FIELD"`

---

#### 4. `test_missing_email_goes_to_error`
**Purpose:**  
Ensure records with missing `email` go to error table.

---

#### 5. `test_missing_name_goes_to_error`
**Purpose:**  
Ensure records with missing `name` go to error table.

---

#### 6. `test_mixed_valid_and_invalid_agents`
**Purpose:**  
Ensure pipeline can handle mixed good and bad data in same batch.

**Checks:**
- 1 valid record in `final_df`
- 1 invalid record in `error_df`

---

## 2. Roles Pipeline

### File
src/pipelines/roles_pipeline.py


### What the pipeline does
- Reads roles data
- Normalizes:
  - `id` → `role_id`
  - `default` → `is_default`
- Writes to `stg_roles`

### Testing Approach
Roles pipeline is straightforward mapping.  
We test:
- Schema correctness
- Row count
- Null handling (if needed in future)

---

## 3. Agent-Roles Pipeline

### File
src/pipelines/agent_roles_pipeline.py


### What the pipeline does
- Reads agents and roles
- Normalizes agent type
- Joins agents to roles based on `agent_type`
- Writes mapping to `stg_agent_roles`

### Testing Focus
- Correct join behavior
- Only valid combinations are produced
- No duplicates

---

## 4. Groups Pipeline

### File
src/pipelines/groups_pipeline.py
tests/test_groups_transform.py


### What the pipeline does

1. Reads `admin_groups`
2. Creates:
   - `stg_groups`
   - `stg_group_membership`
3. Explodes `agent_ids`
4. Loads valid agents from DB
5. Splits:
   - **valid membership**
   - **invalid membership**
6. Writes invalid records to `group_pipeline_errors`

---

## Groups Test Cases Explained

### 1. `test_valid_group_and_membership`

**Purpose:**  
Ensure valid groups and valid agent IDs are processed correctly.

**Checks:**
- `groups_df` has records
- `membership_df` has records
- `error_df` is empty

---

### 2. `test_invalid_agent_in_group_goes_to_error`

**Purpose:**  
Ensure invalid agent IDs go to error table.

**Checks:**
- `valid_membership_df` is empty
- `error_df` has 1 record
- `error_type == "INVALID_AGENT_ID"`

---

### 3. `test_mixed_valid_and_invalid_membership`

**Purpose:**  
Ensure both valid and invalid memberships are handled in same batch.

**Checks:**
- 1 valid membership
- 1 error record

---

## How This Fits in CI/CD

In CI pipeline:
```bash
pytest tests/

This ensures:

No broken transformations

No regression

Safe code before merge