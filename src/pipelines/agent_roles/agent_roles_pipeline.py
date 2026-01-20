import argparse

from src.db.connection import get_postgres_properties
from src.spark.spark_session import get_spark_session
from src.pipelines.agent_roles.agent_roles_ingestion import read_agent_roles_inputs
from src.pipelines.agent_roles.agent_roles_transform import transform_agent_roles


def run_agent_roles_pipeline(day: str):
    print(f"=== Starting agent roles pipeline for {day} ===")

    spark = get_spark_session("agent-roles-pipeline")

    # -----------------------
    # Ingestion
    # -----------------------
    agents_df, roles_df = read_agent_roles_inputs(spark, day)

    # -----------------------
    # Transformation
    # -----------------------
    agent_roles_df = transform_agent_roles(agents_df, roles_df)


    # -----------------------
    # Write to Postgres (staging)
    # -----------------------
    db_properties = get_postgres_properties()
    jdbc_url = db_properties["url"]

    agent_roles_df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, "stg_agent_roles", properties=db_properties)

    print("Data written to stg_agent_roles")
    print(f"Agent-Roles pipeline completed successfully for {day}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day (e.g. day1, day2)")
    args = parser.parse_args()

    run_agent_roles_pipeline(args.day)
