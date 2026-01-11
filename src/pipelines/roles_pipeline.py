# src/pipelines/roles_pipeline.py

import argparse
import os
from pyspark.sql.functions import col, lit, to_json, struct
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader


def transform_roles(roles_df):
    """
    Core transformation logic for roles pipeline.
    This is what we unit test.
    """

    # -----------------------------------
    # ERROR DETECTION – missing critical fields
    # -----------------------------------
    error_df = roles_df.filter(
        col("id").isNull() |
        col("name").isNull()
    ).select(
        col("id").alias("role_id"),
        lit("MISSING_REQUIRED_FIELD").alias("error_type"),
        lit("Role id or name is null").alias("error_message"),
        to_json(struct("*")).alias("raw_record")
    )

    # -----------------------------------
    # CLEAN DATA ONLY
    # -----------------------------------
    clean_df = roles_df.filter(
        col("id").isNotNull() &
        col("name").isNotNull()
    )

    # -----------------------------------
    # NORMALIZE FIELDS
    # -----------------------------------
    final_df = clean_df.select(
        col("id").cast("long").alias("role_id"),
        col("name"),
        col("description"),
        col("default").alias("is_default"),
        col("agent_type"),
        col("created_at"),
        col("updated_at")
    )

    return final_df, error_df


def run_roles_pipeline(day: str):
    print(f"=== Starting roles pipeline for {day} ===")

    spark = get_spark_session("roles-pipeline")
    reader = DataReader(spark)

    input_path = os.path.join("sample_data", f"sync-{day}", "roles")
    print(f"Reading roles from: {input_path}")

    roles_df = reader.read(input_path, "json")

    final_df, error_df = transform_roles(roles_df)

    print("Final Roles Preview:")
    final_df.show(truncate=False)

    # -----------------------------------
    # WRITE TO POSTGRES (STAGING)
    # -----------------------------------
    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"
    db_properties = {
        "user": "rithvik_zluri_pipeline_user",
        "password": "rithvik_zluri_pipeline_pass",
        "driver": "org.postgresql.Driver"
    }

    final_df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, "stg_roles", properties=db_properties)

    # -----------------------------------
    # WRITE ERRORS
    # -----------------------------------
    error_count = error_df.count()
    if error_count > 0:
        error_df.write \
            .mode("append") \
            .jdbc(jdbc_url, "role_pipeline_errors", properties=db_properties)

    print(f"✅ Roles pipeline completed successfully for {day}")
    print(f"⚠️ Error records written: {error_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day (e.g. day1, day2)")
    args = parser.parse_args()

    run_roles_pipeline(args.day)
