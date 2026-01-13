import argparse
from src.spark.spark_session import get_spark_session
from pyspark.sql.functions import col, explode, to_date, lit, current_timestamp
from src.utils.reader import DataReader
from src.db.connection import get_postgres_properties
from src.pipelines.budgets.budgets_ingestion import read_budgets_raw
from src.pipelines.budgets.budgets_transform import transform_budgets


def run_budgets_pipeline(day: str):
    print(f"=== Starting budgets pipeline for {day} ===")

    spark = get_spark_session("budgets-pipeline")
    reader = DataReader(spark)

    db_props = get_postgres_properties()
    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"

    # ---------------------------
    # INGESTION
    # ---------------------------
    raw_df = read_budgets_raw(reader, day)

    # ---------------------------
    # TRANSFORM
    # ---------------------------
    valid_df, error_df = transform_budgets(raw_df, day)

    # ---------------------------
    # IDEMPOTENCY (filter existing sync_day)
    # ---------------------------
    try:
        existing_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "stg_budgets") \
            .option("user", db_props["user"]) \
            .option("password", db_props["password"]) \
            .option("driver", db_props["driver"]) \
            .load()

        valid_df = valid_df.join(
            existing_df.filter(col("sync_day") == day).select("budget_id"),
            on="budget_id",
            how="left_anti"
        )

    except Exception:
        print("stg_budgets does not exist yet, skipping idempotency filter")

    # ---------------------------
    # WRITE STAGING
    # ---------------------------
    valid_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "stg_budgets") \
        .option("user", db_props["user"]) \
        .option("password", db_props["password"]) \
        .option("driver", db_props["driver"]) \
        .mode("append") \
        .save()

    print("Data appended to stg_budgets")

    # ---------------------------
    # WRITE ERRORS
    # ---------------------------
    if not error_df.rdd.isEmpty():
        error_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "budget_pipeline_errors") \
            .option("user", db_props["user"]) \
            .option("password", db_props["password"]) \
            .option("driver", db_props["driver"]) \
            .option("stringtype", "unspecified") \
            .mode("append") \
            .save()


        print("Error records written")

    print(f"=== Budgets pipeline completed for {day} ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day folder, e.g. day1, day2")
    args = parser.parse_args()

    run_budgets_pipeline(args.day)
