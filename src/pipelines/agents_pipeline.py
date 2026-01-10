import argparse
import os
from pyspark.sql.functions import col, lit, to_json, struct, when
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader


def run_agents_pipeline(day: str):
    print(f"=== Starting agents pipeline for {day} ===")

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
    # ERROR DETECTION – missing critical fields
    # -----------------------------------
    error_df = agent_details_df.filter(
        col("id").isNull() |
        col("contact.email").isNull() |
        col("contact.name").isNull()
    ).select(
        col("id").alias("agent_id"),
        lit("MISSING_REQUIRED_FIELD").alias("error_type"),
        lit("One or more required fields are null").alias("error_message"),
        to_json(struct("*")).alias("raw_record")
    )

    # -----------------------------------
    # CLEAN DATA ONLY
    # -----------------------------------
    clean_df = agent_details_df.filter(
        col("id").isNotNull() &
        col("contact.email").isNotNull() &
        col("contact.name").isNotNull()
    )

    # -----------------------------------
    # ALL AGENTS (from agent_details)
    # -----------------------------------
    all_agents_df = clean_df.select(
        col("id").cast("long").alias("agent_id"),
        col("contact.email").alias("email"),
        col("contact.name").alias("name"),
        col("contact.job_title").alias("job_title"),
        col("contact.language").alias("language"),
        col("contact.mobile").alias("mobile"),
        col("contact.phone").alias("phone"),
        col("contact.time_zone").alias("time_zone"),
        col("available"),
        col("deactivated"),  # KEEP AS IS
        col("focus_mode"),
        col("agent_operational_status"),
        col("last_active_at").cast("timestamp"),
        col("created_at").cast("timestamp"),
        col("updated_at").cast("timestamp")
    )

    print("All agents (from agent_details):")
    all_agents_df.select("agent_id", "email").show(truncate=False)

    # -----------------------------------
    # ACTIVE AGENTS (from agents/*.json)
    # -----------------------------------
    active_agents_df = agents_df.select(
        col("id").cast("long").alias("agent_id")
    ).withColumn("is_active", lit(True))

    print("Active agents (from agents list):")
    active_agents_df.show(truncate=False)

    # -----------------------------------
    # JOIN → DERIVE STATUS
    # -----------------------------------
    final_agents_df = all_agents_df.join(
        active_agents_df,
        on="agent_id",
        how="left"
    ).withColumn(
        "status",
        when(col("is_active").isNotNull(), lit("active")).otherwise(lit("inactive"))
    ).drop("is_active")

    print("Final Agents Preview (with status):")
    final_agents_df.select("agent_id", "deactivated", "status").orderBy("agent_id").show(truncate=False)

    # -----------------------------------
    # WRITE TO POSTGRES (STAGING)
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

    # -----------------------------------
    # WRITE ERRORS
    # -----------------------------------
    error_count = error_df.count()
    if error_count > 0:
        error_df.write \
            .mode("append") \
            .jdbc(jdbc_url, "agent_pipeline_errors", properties=db_properties)

    print(f"✅ Agents pipeline completed successfully for {day}")
    print(f"⚠️ Error records written: {error_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day (e.g. day1, day2)")
    args = parser.parse_args()

    run_agents_pipeline(args.day)
