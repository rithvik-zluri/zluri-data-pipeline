1️⃣ Why we read agent_details and agents separately
Reasoning
agent_details = all agents ever known (master list)

agents folder = currently active agents in this sync

By comparing both, we can detect deactivated agents correctly across days.

This is state management. Without this, you can’t know who became inactive on day2.

2️⃣ Why we calculate deactivated in Spark (not SQL)
Logic
deactivated = is_active IS NULL
Reasoning
If agent is missing from agents/dayX folder → means Freshchat marked them inactive.

Doing this in Spark:

keeps SQL simple

avoids complex NOT IN logic

ensures day1 → day2 transition is accurate

3️⃣ Why we write to stg_agents first (staging)
Reasoning
Staging tables give:

Idempotency (safe re-runs)

Debuggability (you can SELECT * FROM stg_agents)

Clean separation between ingest and business logic

This is industry standard ETL design.

4️⃣ Why we use INSERT … ON CONFLICT (upsert)
Reasoning
We need:

Day1 → insert

Day2 → update existing rows

Day3 → update again

So:

INSERT INTO agents (...)
SELECT ... FROM stg_agents
ON CONFLICT (agent_id)
DO UPDATE SET ...
This makes the pipeline:

Incremental

Idempotent

Safe for re-runs

5️⃣ Why we have agent_status_history
Reasoning
The agents table only stores latest state.
History is lost if we only update.

So we store:

agent_id | sync_date | is_active
This gives:

Trend analysis

Auditing

“Who was active yesterday vs today”

This is slowly changing dimension (Type 2 light) pattern.

6️⃣ Why sync_date is parameterized / CURRENT_DATE
Reasoning
In production → CURRENT_DATE

In simulation/testing → pass date manually

This allows:

Proper day1 → day2 → day3 simulation

No overwriting history

✅ Final Correct Execution Order (IMPORTANT)
Always run in this order 👇

Step 1 – Activate venv
source venv/bin/activate
Step 2 – Run Spark pipeline (loads staging)
Day 1
python -m src.pipelines.agents_pipeline --day day1
Day 2
python -m src.pipelines.agents_pipeline --day day2
Step 3 – Run SQL upserts + history snapshot
Normal (real run)
psql -d rithvik_zluri_pipeline_db -f src/db/migrations/010_upsert_agents.sql
Simulating a specific day (for testing)
psql -d rithvik_zluri_pipeline_db -v sync_date="'2026-01-08'" -f src/db/migrations/010_upsert_agents.sql
7️⃣ How to verify (important)
Check latest state
SELECT agent_id, deactivated FROM agents ORDER BY agent_id;
Check history across days
SELECT * 
FROM agent_status_history
ORDER BY sync_date, agent_id;
Compare yesterday vs today
SELECT
  agent_id,
  bool_or(is_active) FILTER (WHERE sync_date = CURRENT_DATE - INTERVAL '1 day') AS yesterday_active,
  bool_or(is_active) FILTER (WHERE sync_date = CURRENT_DATE) AS today_active
FROM agent_status_history
GROUP BY agent_id
ORDER BY agent_id;