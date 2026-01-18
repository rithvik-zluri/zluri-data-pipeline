from prefect import task, get_run_logger

from src.db.connection import run_sql_file
from src.pipelines.agents.agents_pipeline import run_agents_pipeline
from src.pipelines.roles.roles_pipeline import run_roles_pipeline
from src.pipelines.agent_roles.agent_roles_pipeline import run_agent_roles_pipeline
from src.pipelines.groups.groups_pipeline import run_groups_pipeline
from src.pipelines.budgets.budgets_pipeline import run_budgets_pipeline
from src.pipelines.cards.cards_pipeline import run_cards_pipeline
from src.pipelines.transactions.transactions_pipeline import run_transactions_pipeline


# -------------------------
# SQL runner task
# -------------------------
@task(name="run_sql")
def run_sql(sql_file: str, day: str):
    logger = get_run_logger()
    logger.info(f"🗄️ Running SQL: {sql_file} for {day}")
    run_sql_file(sql_file, day=day)


# -------------------------
# Pipeline Tasks
# -------------------------

@task(name="agents_task")
def agents_task(day: str):
    run_agents_pipeline(day)
    run_sql("src/db/migrations/010_upsert_agents.sql", day)


@task(name="roles_task")
def roles_task(day: str):
    run_roles_pipeline(day)
    run_sql("src/db/migrations/011_upsert_roles.sql", day)


@task(name="agent_roles_task")
def agent_roles_task(day: str):
    run_agent_roles_pipeline(day)
    run_sql("src/db/migrations/012_upsert_agent_roles.sql", day)


@task(name="groups_task")
def groups_task(day: str):
    run_groups_pipeline(day)
    run_sql("src/db/migrations/020_upsert_groups.sql", day)


@task(name="budgets_task")
def budgets_task(day: str):
    run_budgets_pipeline(day)
    run_sql("src/db/migrations/030_upsert_budgets.sql", day)


@task(name="cards_task")
def cards_task(day: str):
    run_cards_pipeline(day)
    run_sql("src/db/migrations/031_upsert_cards.sql", day)


@task(name="transactions_task")
def transactions_task(day: str):
    run_transactions_pipeline(day)
    run_sql("src/db/migrations/040_upsert_transactions.sql", day)
