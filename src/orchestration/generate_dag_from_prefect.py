from datetime import date
from src.orchestration.prefect_flow import zluri_full_pipeline_flow


def generate_dag():
    """
    Generate a DAG visualization for the Prefect flow.
    Uses dummy parameters only for graph construction.
    """
    zluri_full_pipeline_flow.visualize(
        day="day2",
        sync_date=date(2026, 1, 19)
    )

    print("DAG visualization generated successfully")


if __name__ == "__main__":
    generate_dag()
