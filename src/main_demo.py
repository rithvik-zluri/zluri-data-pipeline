from pathlib import Path

from src.db.connection import run_sql_file
from src.pipelines.agents_pipeline import run_agents_pipeline
from src.pipelines.roles_pipeline import run_roles_pipeline
from src.pipelines.agent_roles_pipeline import run_agent_roles_pipeline
from src.pipelines.groups_pipeline import run_groups_pipeline

BASE_DIR = Path(__file__).resolve().parent


def run_sql(relative_path: str):
    full_path = BASE_DIR / relative_path
    print(f"🗄️ Running SQL: {full_path}")
    run_sql_file(str(full_path))


def run_full_demo():
    print("\n==============================")
    print("🎯 ZLURI DATA PIPELINE DEMO")
    print("==============================\n")

    # 1. Create tables (safe)
    print("🛠️ Creating tables if not exist...")
    run_sql("db/migrations/001_create_tables.sql")

    # 2. Skip truncate/reset (due to FK & permission issues)
    print("⚠️ Skipping truncate/reset to avoid FK & permission issues")

    # -------- DAY 1 SYNC --------
    print("\n==============================")
    print("📅 DAY 1 SYNC START")
    print("==============================")

    # --- Run pipelines (write to staging tables) ---
    run_agents_pipeline("day1")
    run_roles_pipeline("day1")
    run_agent_roles_pipeline("day1")
    run_groups_pipeline("day1")

    # --- Upsert into final tables ---
    print("\n🔄 Upserting DAY 1 data into final tables...")
    run_sql("db/migrations/010_upsert_agents.sql")
    run_sql("db/migrations/011_upsert_roles.sql")
    run_sql("db/migrations/012_upsert_agent_roles.sql")
    run_sql("db/migrations/020_upsert_groups.sql")

    print("\n✅ DAY 1 SYNC + UPSERT COMPLETE")

    # -------- DAY 2 SYNC --------
    print("\n==============================")
    print("📅 DAY 2 SYNC START")
    print("==============================")

    # --- Run pipelines (write to staging tables) ---
    run_agents_pipeline("day2")
    run_roles_pipeline("day2")
    run_agent_roles_pipeline("day2")
    run_groups_pipeline("day2")

    # --- Upsert into final tables ---
    print("\n🔄 Upserting DAY 2 data into final tables...")
    run_sql("db/migrations/010_upsert_agents.sql")
    run_sql("db/migrations/011_upsert_roles.sql")
    run_sql("db/migrations/012_upsert_agent_roles.sql")
    run_sql("db/migrations/020_upsert_groups.sql")

    print("\n✅ DAY 2 SYNC + UPSERT COMPLETE")

    print("\n==============================")
    print("🎉 DEMO PIPELINE RUN FINISHED")
    print("👉 Final tables now reflect DAY 2 state via upserts")
    print("==============================\n")


if __name__ == "__main__":
    run_full_demo()
