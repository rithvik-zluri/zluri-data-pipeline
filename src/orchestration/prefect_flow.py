from prefect import flow
from src.orchestration.prefect_tasks import (
    agents_pipeline,
    roles_pipeline,
    agent_roles_pipeline,
    groups_pipeline,
    budgets_pipeline,
    cards_pipeline,
    transactions_pipeline,
    upsert_agents,
    upsert_roles,
    upsert_agent_roles,
    upsert_groups,
    upsert_budgets,
    upsert_cards,
    upsert_transactions,
)
from src.spark.spark_session import get_spark_session


@flow(name="zluri-day-pipeline")
def zluri_day_pipeline(day: str):

    # ------------------
    # Core dimensions
    # ------------------
    agents = agents_pipeline.submit(day)
    roles = roles_pipeline.submit(day)

    agents_upsert = upsert_agents.submit(day, wait_for=[agents])
    roles_upsert = upsert_roles.submit(day, wait_for=[roles])

    # ------------------
    # Agent ↔ Role mapping
    # ------------------
    agent_roles = agent_roles_pipeline.submit(
        day,
        wait_for=[agents_upsert, roles_upsert],
    )
    agent_roles_upsert = upsert_agent_roles.submit(
        day,
        wait_for=[agent_roles],
    )

    # ------------------
    # Groups
    # ------------------
    groups = groups_pipeline.submit(day, wait_for=[agents_upsert])
    groups_upsert = upsert_groups.submit(day, wait_for=[groups])

    # ------------------
    # Financial entities
    # ------------------
    budgets = budgets_pipeline.submit(day)
    cards = cards_pipeline.submit(day)

    budgets_upsert = upsert_budgets.submit(day, wait_for=[budgets])
    cards_upsert = upsert_cards.submit(day, wait_for=[cards])

    # ------------------
    # Transactions
    # ------------------
    transactions = transactions_pipeline.submit(
        day,
        wait_for=[budgets_upsert, cards_upsert],
    )
    transactions_upsert = upsert_transactions.submit(
        day,
        wait_for=[transactions],
    )

    # ------------------
    # 🔥 HARD WAIT FOR EVERYTHING
    # ------------------
    all_futures = [
        agents,
        roles,
        agents_upsert,
        roles_upsert,
        agent_roles,
        agent_roles_upsert,
        groups,
        groups_upsert,
        budgets,
        budgets_upsert,
        cards,
        cards_upsert,
        transactions,
        transactions_upsert,
    ]

    for f in all_futures:
        f.result()

    # ------------------
    # STOP SPARK (SAFE)
    # ------------------
    spark = get_spark_session()
    spark.stop()
    print("Spark session stopped cleanly ✅")



# =========================
# BACKFILL FLOW
# =========================
@flow(name="zluri-backfill")
def zluri_backfill(days: list[str]):
    for day in days:
        zluri_day_pipeline(day)


# =========================
# CLI ENTRYPOINT
# =========================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--day", type=str)
    parser.add_argument("--backfill", nargs="+")
    args = parser.parse_args()

    if args.backfill:
        zluri_backfill(days=args.backfill)
    elif args.day:
        zluri_day_pipeline(day=args.day)
    else:
        raise ValueError("Provide --day or --backfill")
