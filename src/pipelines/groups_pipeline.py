from pyspark.sql.functions import col, explode, lit
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader
import argparse
import os

def run_groups_pipeline(day: str):
    spark = get_spark_session("groups-pipeline")
    reader = DataReader(spark)

    base_path = os.path.join("sample_data", f"sync-{day}", "admin_groups")

    print(f"Reading groups from: {base_path}")
    groups_df = reader.read(base_path, file_format="json")

    # -------------------------------
    # FLATTEN GROUPS
    # -------------------------------
    flat_groups_df = groups_df.select(
        col("id").alias("group_id"),
        col("name"),
        lit(None).cast("long").alias("parent_group_id"),  # ✅ FIX: no parent in source
        col("created_at").cast("timestamp"),
        col("updated_at").cast("timestamp")
    )


    # -------------------------------
    # FLATTEN MEMBERSHIPS
    # -------------------------------
    membership_df = groups_df.select(
        col("id").alias("group_id"),
        explode(col("user_ids")).alias("agent_id")
    )

    print("Groups Preview:")
    flat_groups_df.show(truncate=False)

    print("Membership Preview:")
    membership_df.show(truncate=False)

    # -------------------------------
    # WRITE TO STAGING
    # -------------------------------
    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"
    db_properties = {
        "user": "rithvik_zluri_pipeline_user",
        "password": "rithvik_zluri_pipeline_pass",
        "driver": "org.postgresql.Driver"
    }

    flat_groups_df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, "stg_groups", properties=db_properties)

    membership_df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, "stg_group_membership", properties=db_properties)

    print(f"✅ Groups pipeline completed successfully for {day}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day (e.g. day1, day2)")
    args = parser.parse_args()

    run_groups_pipeline(args.day)
