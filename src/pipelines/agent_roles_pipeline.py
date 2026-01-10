from pyspark.sql.functions import col, explode
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader
from src.db.connection import get_postgres_properties
import argparse


def run_agent_roles_pipeline(day: str):
    spark = get_spark_session("agent_roles_pipeline")
    reader = DataReader(spark)

    base_path = f"sample_data/sync-{day}/agent_details"
    print(f"Reading agent_details from: {base_path}")

    df = reader.read(base_path, "json")

    # ===============================
    # Build current agent-role mapping
    # ===============================
    current_roles_df = df.select(
        col("id").cast("bigint").alias("agent_id"),
        explode(col("role_ids")).alias("role_id")
    ).select(
        col("agent_id"),
        col("role_id").cast("bigint")
    ).dropDuplicates()

    print("Current agent-role mappings:")
    current_roles_df.show(truncate=False)

    # ===============================
    # Write to staging table
    # ===============================
    db_props = get_postgres_properties()
    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"

    current_roles_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "stg_agent_roles") \
        .option("user", db_props["user"]) \
        .option("password", db_props["password"]) \
        .option("driver", db_props["driver"]) \
        .mode("overwrite") \
        .save()

    print("✅ stg_agent_roles written successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day (e.g. day1, day2)")
    args = parser.parse_args()

    run_agent_roles_pipeline(args.day)
