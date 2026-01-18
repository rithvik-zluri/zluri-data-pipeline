from pathlib import Path
from datetime import date

from src.db.connection import run_sql_file
from src.pipelines.agents.agents_pipeline import run_agents_pipeline
from src.pipelines.roles.roles_pipeline import run_roles_pipeline
from src.pipelines.agent_roles.agent_roles_pipeline import run_agent_roles_pipeline
from src.pipelines.groups.groups_pipeline import run_groups_pipeline
from src.pipelines.budgets.budgets_pipeline import run_budgets_pipeline
from src.pipelines.cards.cards_pipeline import run_cards_pipeline
from src.pipelines.transactions.transactions_pipeline import run_transactions_pipeline

BASE_DIR = Path(__file__).resolve().parent


# =========================
# SQL RUNNER (NO VARIABLES)
# =========================
def run_sql(relative_path: str):
    full_path = BASE_DIR / relative_path
    print(f"🗄️ Running SQL: {full_path}")
    run_sql_file(str(full_path))


# =========================
# DAY RUNNER
# =========================
def run_day(day: str, sync_date: date):
    print(f"\n==============================")
    print(f"{day.upper()} SYNC START")
    print(f"Logical sync date: {sync_date}")
    print("==============================")

    # ----------------------------
    # PHASE 1: CORE DIMENSIONS
    # ----------------------------
    run_agents_pipeline(day)
    run_sql("db/migrations/010_upsert_agents.sql")

    run_roles_pipeline(day)
    run_sql("db/migrations/011_upsert_roles.sql")

    run_agent_roles_pipeline(day)
    run_sql("db/migrations/012_upsert_agent_roles.sql")

    # ----------------------------
    # PHASE 2: DEPENDENT ENTITIES
    # ----------------------------
    run_groups_pipeline(day)
    run_sql("db/migrations/020_upsert_groups.sql")

    run_budgets_pipeline(day)
    run_sql("db/migrations/030_upsert_budgets.sql")

    run_cards_pipeline(day)
    run_sql("db/migrations/031_upsert_cards.sql")

    run_transactions_pipeline(day)
    run_sql("db/migrations/040_upsert_transactions.sql")

    print(f"\n{day.upper()} SYNC COMPLETE")


# =========================
# FULL DEMO
# =========================
def run_full_demo():
    print("\n==============================")
    print("ZLURI DATA PIPELINE DEMO")
    print("==============================\n")

    print("🛠️ Creating tables if not exist...")
    run_sql("db/migrations/001_create_tables.sql")

    print("Skipping truncate/reset to avoid FK & permission issues")

    # -------- DAY 1 --------
    run_day("day1", sync_date=date(2024, 1, 1))

    # -------- DAY 2 --------
    run_day("day2", sync_date=date(2024, 1, 2))

    print("\n==============================")
    print("DEMO PIPELINE RUN FINISHED")
    print("==============================\n")


if __name__ == "__main__":
    run_full_demo()
