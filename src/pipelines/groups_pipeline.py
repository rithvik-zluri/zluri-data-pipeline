import argparse
import os
from pyspark.sql.functions import col, explode, to_timestamp, lit, to_json, struct
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader
from src.db.connection import get_postgres_properties


def run_groups_pipeline(day: str):
    print(f"=== Starting groups pipeline for {day} ===")

    spark = get_spark_session("GroupsPipeline")
    reader = DataReader(spark)

    base_path = os.path.join("sample_data", f"sync-{day}", "admin_groups")
    print(f"Reading groups from: {base_path}")

    raw_df = reader.read(base_path, "json")

    # -----------------------------
    # GROUPS TABLE
    # -----------------------------
    groups_df = raw_df.select(
        col("id").cast("bigint").alias("group_id"),
        col("name"),
        col("parent_group_id").cast("bigint"),
        to_timestamp(col("created_at")).alias("created_at"),
        to_timestamp(col("updated_at")).alias("updated_at"),
    )

    print("Sample groups data:")
    groups_df.show(truncate=False)

    # -----------------------------
    # GROUP MEMBERSHIP (agent_ids)
    # -----------------------------
    exploded_df = raw_df.select(
        col("id").cast("bigint").alias("group_id"),
        explode(col("agent_ids")).alias("agent_id")
    )

    membership_df = exploded_df.select(
        col("group_id"),
        col("agent_id").cast("bigint").alias("agent_id")
    )

    print("Sample raw group membership data:")
    membership_df.show(truncate=False)

    # -----------------------------
    # LOAD VALID AGENTS
    # -----------------------------
    db_props = get_postgres_properties()
    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"

    agents_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "agents") \
        .option("user", db_props["user"]) \
        .option("password", db_props["password"]) \
        .option("driver", db_props["driver"]) \
        .load() \
        .select("agent_id")

    # -----------------------------
    # SPLIT VALID / INVALID MEMBERSHIP
    # -----------------------------
    valid_membership_df = membership_df.join(
        agents_df,
        on="agent_id",
        how="inner"
    )

    invalid_membership_df = membership_df.join(
        agents_df,
        on="agent_id",
        how="left_anti"
    )

    print("Valid membership records:")
    valid_membership_df.show(truncate=False)

    print("Invalid membership records:")
    invalid_membership_df.show(truncate=False)

    # -----------------------------
    # ERROR RECORDS (JSONB FIXED)
    # -----------------------------
    error_df = invalid_membership_df.select(
        col("group_id"),
        lit("INVALID_AGENT_ID").alias("error_type"),
        lit("agent_id does not exist in agents table").alias("error_message"),
        to_json(struct("group_id", "agent_id")).alias("raw_record")  # stays JSON string
    )

    # -----------------------------
    # WRITE stg_groups
    # -----------------------------
    groups_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "stg_groups") \
        .option("user", db_props["user"]) \
        .option("password", db_props["password"]) \
        .option("driver", db_props["driver"]) \
        .mode("overwrite") \
        .save()

    print("✅ Data written to stg_groups")

    # -----------------------------
    # WRITE stg_group_membership (ONLY VALID)
    # -----------------------------
    valid_membership_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "stg_group_membership") \
        .option("user", db_props["user"]) \
        .option("password", db_props["password"]) \
        .option("driver", db_props["driver"]) \
        .mode("overwrite") \
        .save()

    print("✅ Valid data written to stg_group_membership")

    # -----------------------------
    # WRITE ERRORS (JSONB SAFE)
    # -----------------------------
    error_count = error_df.count()

    if error_count > 0:
        error_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "group_pipeline_errors") \
            .option("user", db_props["user"]) \
            .option("password", db_props["password"]) \
            .option("driver", db_props["driver"]) \
            .option("stringtype", "unspecified") \
            .mode("append") \
            .save()

    print(f"⚠️ Error records written: {error_count}")
    print(f"✅ Groups pipeline completed successfully for {day}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day folder, e.g. day1, day2")
    args = parser.parse_args()

    run_groups_pipeline(args.day)
