from pyspark.sql.functions import col, lit
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader
import argparse
import os


def run_agents_pipeline(day: str):
    spark = get_spark_session("agents-pipeline")
    reader = DataReader(spark)

    base_path = os.path.join("sample_data", f"sync-{day}")

    agent_details_path = os.path.join(base_path, "agent_details")
    agents_path = os.path.join(base_path, "agents")

    print(f"Reading agent details from: {agent_details_path}")
    agent_details_df = reader.read(agent_details_path, file_format="json")

    print(f"Reading agents list from: {agents_path}")
    agents_df = reader.read(agents_path, file_format="json")

    # -----------------------------------
    # ALL AGENTS (from agent_details)
    # -----------------------------------
    all_agents_df = agent_details_df.select(
        col("id").alias("agent_id"),
        col("contact.email").alias("email"),
        col("contact.name").alias("name"),
        col("contact.job_title").alias("job_title"),
        col("contact.language").alias("language"),
        col("contact.mobile").alias("mobile"),
        col("contact.phone").alias("phone"),
        col("contact.time_zone").alias("time_zone"),
        col("available"),
        col("focus_mode"),
        col("agent_operational_status"),
        col("last_active_at").cast("timestamp"),
        col("created_at").cast("timestamp"),
        col("updated_at").cast("timestamp")
    )

    # -----------------------------------
    # ACTIVE AGENTS (from agents/*.json)
    # -----------------------------------
    active_agents_df = agents_df.select(
        col("id").alias("agent_id")
    ).withColumn("is_active", lit(True))

    # -----------------------------------
    # JOIN → DETECT INACTIVE
    # -----------------------------------
    final_agents_df = all_agents_df.join(
        active_agents_df,
        on="agent_id",
        how="left"
    ).withColumn(
        "deactivated",
        col("is_active").isNull()  # if not present in active list → deactivated = true
    ).drop("is_active")

    print("Final Agents Preview:")
    final_agents_df.select("agent_id", "deactivated").orderBy("agent_id").show(truncate=False)

    # -----------------------------------
    # WRITE TO POSTGRES STAGING
    # -----------------------------------
    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"
    db_properties = {
        "user": "rithvik_zluri_pipeline_user",
        "password": "rithvik_zluri_pipeline_pass",
        "driver": "org.postgresql.Driver"
    }

    final_agents_df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, "stg_agents", properties=db_properties)

    print(f"✅ Agents pipeline completed successfully for {day}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day (e.g. day1, day2)")
    args = parser.parse_args()

    run_agents_pipeline(args.day)
