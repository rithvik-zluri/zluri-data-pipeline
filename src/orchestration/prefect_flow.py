from prefect import flow

from src.orchestration.prefect_tasks import (
    agents_task,
    roles_task,
    agent_roles_task,
    groups_task,
    budgets_task,
    cards_task,
    transactions_task,
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
    agents = agents_task(day)
    roles = roles_task(day)

    # ------------------
    # Dependent mappings
    # ------------------
    agent_roles_task(day, wait_for=[agents, roles])

    # ------------------
    # Groups depend on agents
    # ------------------
    groups_task(day, wait_for=[agents])

    # ------------------
    # Financial entities
    # ------------------
    budgets = budgets_task(day)
    cards = cards_task(day)

    # ------------------
    # Transactions depend on budgets & cards
    # ------------------
    transactions_task(day, wait_for=[budgets, cards])


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
