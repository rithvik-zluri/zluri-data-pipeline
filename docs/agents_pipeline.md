Overview
The Agents Pipeline is responsible for ingesting agent data from Freshchat-style JSON files, flattening nested structures, and loading the data into PostgreSQL in a normalized, analytics-friendly format.

It supports:

Schema normalization (flattening nested JSON)

Separation of core agent data, agent details, and availability

Incremental processing across syncs (day1, day2, …)

Inactive agent detection (state management across syncs)

Idempotent loads using staging + upsert pattern

Source Data
Directory Structure
sample_data/
  ├── sync-day1/
  │     └── agent_details/
  │            ├── file1.json
  │            └── file2.json
  └── sync-day2/
        └── agent_details/
               ├── file1.json
               └── file2.json
Raw JSON Structure
Each JSON file contains agent records with fields such as:

id

contact { ... }

availability [ { available, available_since, channel } ]

group_ids

role_ids

org_agent_id

ticket_scope

signature

freshchat_agent

status flags & timestamps

Target Tables (Normalized Schema)
The raw nested JSON is not stored as-is. Instead, it is flattened into the following relational tables:

1. agents (Core Agent Info)
Column	Source
agent_id	id
email	contact.email
name	contact.name
job_title	contact.job_title
language	contact.language
mobile	contact.mobile
phone	contact.phone
time_zone	contact.time_zone
available	available
deactivated	deactivated
focus_mode	focus_mode
agent_operational_status	agent_operational_status
last_active_at	last_active_at
created_at	created_at
updated_at	updated_at
2. agent_details (Extended Metadata)
Column	Source
agent_id	id
org_agent_id	org_agent_id
ticket_scope	ticket_scope
signature	signature
freshchat_agent	freshchat_agent
is_active	contact.active
avatar	contact.avatar
last_login_at	contact.last_login_at
created_at	created_at
updated_at	updated_at
3. agent_availability (Flattened from Array)
Derived by exploding the availability array.

Column	Source
agent_id	id
is_available	availability.available
available_since	availability.available_since
channel	availability.channel
Each availability entry becomes one row per agent per channel.

4. agent_group_membership
Derived by exploding group_ids.

Column	Source
agent_id	id
group_id	group_ids[*]
5. agent_role_mapping
Derived by exploding role_ids.

Column	Source
agent_id	id
role_id	role_ids[*]
Pipeline Flow
Step 1 – Read Raw JSON (Day Based)
The pipeline accepts a day argument to support incremental syncs.

python -m src.pipelines.agents_pipeline --day day1
python -m src.pipelines.agents_pipeline --day day2
Internally this resolves to:

sample_data/sync-day1/agent_details/
sample_data/sync-day2/agent_details/
Step 2 – Transform & Flatten
Transformations applied:

Select & rename fields

Flatten contact struct

Explode:

availability

group_ids

role_ids

Cast timestamps to timestamp type

Step 3 – Load into Staging Tables
Data is first written into staging tables:

stg_agents

stg_agent_details

stg_agent_availability

Using:

.mode("overwrite")
This ensures:

Clean state per run

Idempotent behavior

Safe incremental logic

Step 4 – Upsert into Final Tables
Using 010_upsert_agents.sql:

a) Upsert Agents
INSERT INTO agents
SELECT * FROM stg_agents
ON CONFLICT (agent_id)
DO UPDATE SET
 email = EXCLUDED.email,
 name = EXCLUDED.name,
 job_title = EXCLUDED.job_title,
 language = EXCLUDED.language,
 mobile = EXCLUDED.mobile,
 phone = EXCLUDED.phone,
 time_zone = EXCLUDED.time_zone,
 available = EXCLUDED.available,
 deactivated = EXCLUDED.deactivated,
 focus_mode = EXCLUDED.focus_mode,
 agent_operational_status = EXCLUDED.agent_operational_status,
 last_active_at = EXCLUDED.last_active_at,
 updated_at = EXCLUDED.updated_at;
b) Upsert Agent Details
INSERT INTO agent_details
SELECT * FROM stg_agent_details
ON CONFLICT (agent_id)
DO UPDATE SET
 org_agent_id = EXCLUDED.org_agent_id,
 ticket_scope = EXCLUDED.ticket_scope,
 signature = EXCLUDED.signature,
 freshchat_agent = EXCLUDED.freshchat_agent,
 is_active = EXCLUDED.is_active,
 avatar = EXCLUDED.avatar,
 last_login_at = EXCLUDED.last_login_at,
 updated_at = EXCLUDED.updated_at;
c) Refresh Availability
DELETE FROM agent_availability
USING stg_agent_availability s
WHERE agent_availability.agent_id = s.agent_id;

INSERT INTO agent_availability
SELECT * FROM stg_agent_availability;
Step 5 – Inactive Agent Logic (State Management)
Agents missing in the current sync are marked inactive:

UPDATE agents
SET deactivated = true,
    updated_at = NOW()
WHERE agent_id NOT IN (
    SELECT agent_id FROM stg_agents
);
This enables:

Day1 → Day2 comparison

Accurate inactive tracking

Stateful sync behavior

How to Run (End-to-End)
# 1. Activate venv
source venv/bin/activate

# 2. Load staging from JSON
python -m src.pipelines.agents_pipeline --day day2

# 3. Apply upserts + inactive logic
psql -d rithvik_zluri_pipeline_db -f src/db/migrations/010_upsert_agents.sql
Sample Successful Output
Agents Schema:
 |-- agent_id: long
 |-- email: string
 |-- name: string
 |-- job_title: string
 |-- language: string
 |-- mobile: string
 |-- phone: string
 |-- time_zone: string
 |-- available: boolean
 |-- deactivated: boolean
 |-- focus_mode: boolean
 |-- agent_operational_status: string
 |-- last_active_at: timestamp
 |-- created_at: timestamp
 |-- updated_at: timestamp

Agent Details Schema:
 |-- agent_id: long
 |-- org_agent_id: string
 |-- ticket_scope: long
 |-- signature: string
 |-- freshchat_agent: boolean
 |-- is_active: boolean
 |-- avatar: string
 |-- last_login_at: string
 |-- created_at: timestamp
 |-- updated_at: timestamp

Availability Schema:
 |-- agent_id: long
 |-- is_available: boolean
 |-- available_since: string
 |-- channel: string

Agents staging load completed successfully for day2.