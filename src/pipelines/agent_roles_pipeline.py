# src/pipelines/agent_roles_pipeline.py

import argparse
from pyspark.sql.functions import col, when
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader


def transform_agent_roles(agents_df, roles_df):
    """
    Core transformation logic for agent-roles pipeline.
    This is what we unit test.
    """

    # ---------------------------
    # Normalize agents
    # ---------------------------
    agents_clean = agents_df.select(
        col("id").cast("long").alias("agent_id"),
        col("type").alias("agent_type_str")
    ).withColumn(
        "agent_type",
        when(col("agent_type_str") == "support_agent", 1)
        .otherwise(3)  # collaborators, etc.
    )

    # ---------------------------
    # Normalize roles
    # ---------------------------
    roles_clean = roles_df.select(
        col("id").cast("long").alias("role_id"),
        col("agent_type").cast("int")
    )

    # ---------------------------
    # Join agents to roles
    # ---------------------------
    agent_roles_df = agents_clean.join(
        roles_clean,
        on="agent_type",
        how="inner"
    ).select(
        col("agent_id"),
        col("role_id")
    )

    return agent_roles_df


def run_agent_roles_pipeline(day: str):
    print(f"=== Starting agent roles pipeline for {day} ===")

    spark = get_spark_session("agent-roles-pipeline")
    reader = DataReader(spark)

    agents_path = f"sample_data/sync-{day}/agents"
    roles_path = f"sample_data/sync-{day}/roles"

    print(f"Reading agents from: {agents_path}")
    agents_df = reader.read(agents_path, "json")

    print(f"Reading roles from: {roles_path}")
    roles_df = reader.read(roles_path, "json")

    agent_roles_df = transform_agent_roles(agents_df, roles_df)

    print("Final Agent-Roles mapping preview:")
    agent_roles_df.show(truncate=False)

    # DB config
    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"
    db_properties = {
        "user": "rithvik_zluri_pipeline_user",
        "password": "rithvik_zluri_pipeline_pass",
        "driver": "org.postgresql.Driver"
    }

    agent_roles_df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, "stg_agent_roles", properties=db_properties)

    print("✅ Data written to stg_agent_roles")
    print(f"✅ Agent-Roles pipeline completed successfully for {day}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day (e.g. day1, day2)")
    args = parser.parse_args()

    run_agent_roles_pipeline(args.day)
