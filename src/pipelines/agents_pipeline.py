# src/pipelines/agents_pipeline.py

import argparse
import os
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader


def ensure_column(df, col_name, default_value=None):
    """
    If column does not exist in df, add it with default_value.
    """
    if col_name not in df.columns:
        return df.withColumn(col_name, F.lit(default_value))
    return df


def transform_agents(agent_details_df, agents_df):
    """
    Core transformation logic for agents pipeline.
    Returns: (final_agents_df, error_df)
    """

    # -------------------------------------------------
    # 1. ERROR DETECTION – missing critical fields
    # -------------------------------------------------
    error_condition = (
        F.col("id").isNull() |
        F.col("contact.email").isNull() |
        F.col("contact.name").isNull()
    )

    error_df = agent_details_df.filter(error_condition).select(
        F.col("id").alias("agent_id"),
        F.lit("MISSING_REQUIRED_FIELD").alias("error_type"),
        F.lit("One or more required fields are null").alias("error_message"),
        F.to_json(F.struct("*")).alias("raw_record")
    )

    # -------------------------------------------------
    # 2. CLEAN DATA ONLY
    # -------------------------------------------------
    clean_df = agent_details_df.filter(~error_condition)

    # -------------------------------------------------
    # 3. SAFELY ADD OPTIONAL COLUMNS IF MISSING
    # -------------------------------------------------
    optional_columns = [
        "available",
        "deactivated",
        "focus_mode",
        "agent_operational_status",
        "last_active_at",
        "created_at",
        "updated_at"
    ]

    for col_name in optional_columns:
        clean_df = ensure_column(clean_df, col_name, None)

    # -------------------------------------------------
    # 4. ALL AGENTS (normalized)
    # -------------------------------------------------
    all_agents_df = clean_df.select(
        F.col("id").cast("long").alias("agent_id"),
        F.col("contact.email").alias("email"),
        F.col("contact.name").alias("name"),
        F.col("contact.job_title").alias("job_title"),
        F.col("contact.language").alias("language"),
        F.col("contact.mobile").alias("mobile"),
        F.col("contact.phone").alias("phone"),
        F.col("contact.time_zone").alias("time_zone"),
        F.col("available"),
        F.col("deactivated"),
        F.col("focus_mode"),
        F.col("agent_operational_status"),
        F.col("last_active_at").cast("timestamp"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp")
    )

    # -------------------------------------------------
    # 5. HANDLE EMPTY agents_df SAFELY
    # -------------------------------------------------
    if agents_df.rdd.isEmpty():
        spark = agent_details_df.sparkSession
        empty_schema = StructType([
            StructField("agent_id", LongType(), True)
        ])
        active_agents_df = spark.createDataFrame([], empty_schema).withColumn("is_active", F.lit(True))
    else:
        active_agents_df = agents_df.select(
            F.col("id").cast("long").alias("agent_id")
        ).withColumn("is_active", F.lit(True))

    # -------------------------------------------------
    # 6. JOIN → DERIVE STATUS
    # -------------------------------------------------
    final_agents_df = all_agents_df.join(
        active_agents_df,
        on="agent_id",
        how="left"
    ).withColumn(
        "status",
        F.when(F.col("is_active").isNotNull(), F.lit("active")).otherwise(F.lit("inactive"))
    ).drop("is_active")

    return final_agents_df, error_df


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

    final_agents_df, error_df = transform_agents(agent_details_df, agents_df)

    print("Final Agents Preview (with status):")
    final_agents_df.select("agent_id", "status").orderBy("agent_id").show(truncate=False)

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
