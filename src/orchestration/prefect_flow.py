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

# =========================
# SINGLE DAY FLOW
# =========================
@flow(name="zluri-day-pipeline")
def zluri_day_pipeline(day: str):
    """
    Runs pipeline for a single logical day (day1, day2, etc.)
    """

    # ------------------
    # Core dimensions
    # ------------------
    agents = agents_pipeline(day)
    roles = roles_pipeline(day)

    # Upsert agents and roles after their respective pipelines are complete
    agents_upsert = upsert_agents(day, wait_for=[agents])
    roles_upsert = upsert_roles(day, wait_for=[roles])

    # ------------------
    # Agent ↔ Role mapping
    # ------------------
    agent_roles = agent_roles_pipeline(day, wait_for=[agents_upsert, roles_upsert])
    upsert_agent_roles(day, wait_for=[agent_roles])

    # ------------------
    # Groups (WAIT for agents_upsert)
    # ------------------
    groups = groups_pipeline(day, wait_for=[agents_upsert])
    upsert_groups(day, wait_for=[groups])

    # ------------------
    # Financial entities (budgets, cards)
    # ------------------
    budgets = budgets_pipeline(day)
    cards = cards_pipeline(day)

    # Upsert financial entities
    upsert_budgets(day, wait_for=[budgets])
    upsert_cards(day, wait_for=[cards])

    # ------------------
    # Transactions
    # ------------------
    transactions = transactions_pipeline(day, wait_for=[budgets, cards])
    upsert_transactions(day, wait_for=[transactions])


# =========================
# BACKFILL FLOW
# =========================
@flow(name="zluri-backfill")
def zluri_backfill(days: list[str]):
    """
    Backfill pipeline for multiple logical days.
    Example: ["day1", "day2", "day3"]
    """
    for day in days:
        zluri_day_pipeline(day)


# =========================
# CLI ENTRYPOINT
# =========================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--day", type=str, help="Run pipeline for one day")
    parser.add_argument(
        "--backfill",
        nargs="+",
        help="Backfill multiple days (e.g. day1 day2 day3)",
    )
    args = parser.parse_args()

    if args.backfill:
        zluri_backfill(days=args.backfill)
    elif args.day:
        zluri_day_pipeline(day=args.day)
    else:
        raise ValueError("Provide --day or --backfill")

