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
# Retry configuration
# -----------------------------
RETRIES = 3
RETRY_DELAY_SECONDS = 30


# =============================
# PIPELINE TASKS
# =============================

@task(name="agents_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def agents_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running agents pipeline for {day}")
    run_agents_pipeline(day)
    return True  # Mandatory, always considered "ran"


@task(name="roles_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def roles_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running roles pipeline for {day}")
    try:
        run_roles_pipeline(day)
        return True  # Only return True if data processed
    except Exception as e:
        logger.warning(f"Roles pipeline skipped or failed: {e}")
        return False  # Optional pipelines return False if skipped


@task(name="agent_roles_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def agent_roles_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running agent_roles pipeline for {day}")
    try:
        run_agent_roles_pipeline(day)
        return True
    except Exception as e:
        logger.warning(f"Agent roles pipeline skipped or failed: {e}")
        return False


@task(name="groups_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def groups_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running groups pipeline for {day}")
    run_groups_pipeline(day)
    return True  # Mandatory


@task(name="budgets_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def budgets_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running budgets pipeline for {day}")
    try:
        run_budgets_pipeline(day)
        return True
    except Exception as e:
        logger.warning(f"Budgets pipeline skipped or failed: {e}")
        return False


@task(name="cards_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def cards_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running cards pipeline for {day}")
    try:
        run_cards_pipeline(day)
        return True
    except Exception as e:
        logger.warning(f"Cards pipeline skipped or failed: {e}")
        return False


@task(name="transactions_pipeline", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def transactions_pipeline(day: str):
    logger = get_run_logger()
    logger.info(f"Running transactions pipeline for {day}")
    run_transactions_pipeline(day)
    return True  # Mandatory


# =============================
# UPSERT TASKS
# =============================

@task(name="upsert_agents", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_agents(day: str):
    run_sql_file("src/db/migrations/010_upsert_agents.sql", day=day)


@task(name="upsert_roles", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_roles(day: str, run_pipeline: bool):
    if run_pipeline:
        run_sql_file("src/db/migrations/011_upsert_roles.sql", day=day)


@task(name="upsert_agent_roles", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_agent_roles(day: str, run_pipeline: bool):
    if run_pipeline:
        run_sql_file("src/db/migrations/012_upsert_agent_roles.sql", day=day)


@task(name="upsert_groups", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_groups(day: str):
    run_sql_file("src/db/migrations/020_upsert_groups.sql", day=day)


@task(name="upsert_budgets", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_budgets(day: str, run_pipeline: bool):
    if run_pipeline:
        run_sql_file("src/db/migrations/030_upsert_budgets.sql", day=day)


@task(name="upsert_cards", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_cards(day: str, run_pipeline: bool):
    if run_pipeline:
        run_sql_file("src/db/migrations/031_upsert_cards.sql", day=day)


@task(name="upsert_transactions", retries=RETRIES, retry_delay_seconds=RETRY_DELAY_SECONDS)
def upsert_transactions(day: str):
    run_sql_file("src/db/migrations/040_upsert_transactions.sql", day=day)
