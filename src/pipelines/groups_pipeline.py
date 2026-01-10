import argparse

from pyspark.sql.functions import col, explode, to_timestamp

from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader
from src.db.connection import get_postgres_properties


def run_groups_pipeline(day: str):
    spark = get_spark_session("GroupsPipeline")
    reader = DataReader(spark)

    base_path = f"sample_data/sync-{day}/admin_groups"
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
        explode(col("agent_ids")).alias("agent_id_raw")
    )

    membership_df = exploded_df.select(
        col("group_id"),
        col("agent_id_raw").cast("bigint").alias("agent_id")
    )

    print("Sample group membership data:")
    membership_df.show(truncate=False)

    db_props = get_postgres_properties()
    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"

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
    # WRITE stg_group_membership
    # -----------------------------
    membership_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "stg_group_membership") \
        .option("user", db_props["user"]) \
        .option("password", db_props["password"]) \
        .option("driver", db_props["driver"]) \
        .mode("overwrite") \
        .save()

    print("✅ Data written to stg_group_membership")
    print("Groups pipeline completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day folder, e.g. day1, day2")
    args = parser.parse_args()

    run_groups_pipeline(args.day)
