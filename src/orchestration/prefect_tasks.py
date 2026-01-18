from prefect import task, get_run_logger

from src.pipelines.agents.agents_pipeline import run_agents_pipeline
from src.pipelines.roles.roles_pipeline import run_roles_pipeline
from src.pipelines.agent_roles.agent_roles_pipeline import run_agent_roles_pipeline
from src.pipelines.groups.groups_pipeline import run_groups_pipeline
from src.pipelines.budgets.budgets_pipeline import run_budgets_pipeline
from src.pipelines.cards.cards_pipeline import run_cards_pipeline
from src.pipelines.transactions.transactions_pipeline import run_transactions_pipeline
from src.db.connection import run_sql_file


# -----------------------------
# Common retry configuration
# -----------------------------
RETRIES = 3
RETRY_DELAY_SECONDS = 30


# =============================
# PIPELINE TASKS (Python only)
# =============================

@task(name="agents_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def agents_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running agents pipeline for {day}")
    run_agents_pipeline(day)


@task(name="roles_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def roles_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running roles pipeline for {day}")
    run_roles_pipeline(day)


@task(name="agent_roles_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def agent_roles_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running agent_roles pipeline for {day}")
    run_agent_roles_pipeline(day)


@task(name="groups_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def groups_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running groups pipeline for {day}")
    run_groups_pipeline(day)


@task(name="budgets_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def budgets_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running budgets pipeline for {day}")
    run_budgets_pipeline(day)


@task(name="cards_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def cards_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running cards pipeline for {day}")
    run_cards_pipeline(day)


@task(name="transactions_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def transactions_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running transactions pipeline for {day}")
    run_transactions_pipeline(day)


# =============================
# UPSERT / POST-SYNC TASKS
# =============================

@task(name="upsert_agents", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_agents():
    logger = get_run_logger()
    logger.info("Upserting agents + marking inactive")
    run_sql_file("src/db/migrations/010_upsert_agents.sql")


@task(name="upsert_roles", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_roles():
    logger = get_run_logger()
    logger.info("Upserting roles")
    run_sql_file("src/db/migrations/011_upsert_roles.sql")


@task(name="upsert_agent_roles", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_agent_roles():
    logger = get_run_logger()
    logger.info("Upserting agent_roles")
    run_sql_file("src/db/migrations/012_upsert_agent_roles.sql")


@task(name="upsert_groups", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_groups():
    logger = get_run_logger()
    logger.info("Upserting groups + hierarchical inactive logic")
    run_sql_file("src/db/migrations/020_upsert_groups.sql")


@task(name="upsert_budgets", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_budgets():
    logger = get_run_logger()
    logger.info("Upserting budgets")
    run_sql_file("src/db/migrations/030_upsert_budgets.sql")


@task(name="upsert_cards", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_cards():
    logger = get_run_logger()
    logger.info("Upserting cards")
    run_sql_file("src/db/migrations/031_upsert_cards.sql")


@task(name="upsert_transactions", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_transactions():
    logger = get_run_logger()
    logger.info("Upserting transactions (incremental)")
    run_sql_file("src/db/migrations/040_upsert_transactions.sql")
