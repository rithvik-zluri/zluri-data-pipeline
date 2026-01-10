import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader
from src.db.connection import get_postgres_properties


def run_roles_pipeline(day: str):
    spark = get_spark_session("RolesPipeline")
    reader = DataReader(spark)

    base_path = f"sample_data/sync-{day}/roles"
    print(f"Reading roles from: {base_path}")

    roles_df = reader.read(base_path, "json")

    # Rename + select required columns as per DB schema
    roles_df = roles_df.select(
        col("id").cast("bigint").alias("role_id"),
        col("name"),
        col("description"),
        col("default").alias("is_default"),
        col("agent_type").cast("int"),
        to_timestamp(col("created_at")).alias("created_at"),
        to_timestamp(col("updated_at")).alias("updated_at"),
    )

    print("Sample roles data:")
    roles_df.show(truncate=False)

    db_props = get_postgres_properties()

    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"

    # Write to staging table
    roles_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "stg_roles") \
        .option("user", db_props["user"]) \
        .option("password", db_props["password"]) \
        .option("driver", db_props["driver"]) \
        .mode("overwrite") \
        .save()

    print("✅ Data written to stg_roles successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day folder, e.g. day1, day2")
    args = parser.parse_args()

    run_roles_pipeline(args.day)
