import argparse
from src.spark.spark_session import get_spark_session
from src.pipelines.agents.agents_ingestion import read_agents_data
from src.pipelines.agents.agents_transform import transform_agents


def run_agents_pipeline(day: str):
    print(f"\n=== Starting agents pipeline for {day} ===")

    spark = get_spark_session("agents-pipeline")

    # -----------------------------
    # INGEST
    # -----------------------------
    agent_details_df, agents_df = read_agents_data(spark, day)



    # -----------------------------
    # TRANSFORM
    # -----------------------------
    final_agents_df, error_df = transform_agents(
        agent_details_df,
        agents_df
    )


    # -----------------------------
    # WRITE TO POSTGRES (STAGING)
    # -----------------------------
    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"
    db_properties = {
        "user": "rithvik_zluri_pipeline_user",
        "password": "rithvik_zluri_pipeline_pass",
        "driver": "org.postgresql.Driver",
    }

    (
        final_agents_df.write
        .mode("overwrite")
        .jdbc(jdbc_url, "stg_agents", properties=db_properties)
    )

    # -----------------------------
    # WRITE ERRORS
    # -----------------------------
    error_count = error_df.count()

    if error_count > 0:
        (
            error_df.write
            .mode("append")
            .jdbc(jdbc_url, "agent_pipeline_errors", properties=db_properties)
        )

    print(f"Agents pipeline completed for {day}")
    print(f"Error records written: {error_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--day",
        required=True,
        help="sync day (e.g. day1, day2)"
    )
    args = parser.parse_args()

    run_agents_pipeline(args.day)
