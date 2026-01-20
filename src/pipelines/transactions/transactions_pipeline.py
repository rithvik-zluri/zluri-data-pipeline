import argparse

from src.db.connection import get_postgres_properties
from src.spark.spark_session import get_spark_session
from src.pipelines.transactions.transactions_ingestion import read_transactions
from src.pipelines.transactions.transactions_transform import transform_transactions

STG_TABLE = "stg_transactions"
ERROR_TABLE = "transaction_pipeline_errors"


# =========================
# SPARK WRITER
# =========================

def write_df_to_postgres(df, table_name):
    if df is None or df.rdd.isEmpty():
        print(f"Skipping write for {table_name}, dataframe is empty")
        return

    db_properties = get_postgres_properties()

    (
        df.write.mode("append").option(
            "stringtype", "unspecified"
        ).jdbc(
            url=db_properties["url"],
            table=table_name,
            properties=db_properties,
        )
    )

    print(f"Written {df.count()} records to {table_name}")


# =========================
# MAIN PIPELINE
# =========================

def run_transactions_pipeline(day: str):
    print("================================================")
    print(f"Starting Transactions Pipeline for {day}")
    print("================================================")

    spark = get_spark_session("transactions-pipeline")

    try:
        # -------------------------------------------------
        # 1. Read raw data
        # -------------------------------------------------
        print("Reading raw transactions...")
        raw_df = read_transactions(spark, day)

        if raw_df is None or raw_df.rdd.isEmpty():
            print("No raw data found. Exiting pipeline.")
            return

        # -------------------------------------------------
        # 2. Transform
        # -------------------------------------------------
        print("Transforming transactions...")
        valid_df, error_df, _ = transform_transactions(raw_df, day)
        # ⬆️ pipeline_state_df is intentionally ignored

        # -------------------------------------------------
        # 3. Write valid records to staging
        # -------------------------------------------------
        print("Writing valid records to stg_transactions...")
        write_df_to_postgres(valid_df, STG_TABLE)

        # -------------------------------------------------
        # 4. Write error records
        # -------------------------------------------------
        print("Writing error records to transaction_pipeline_errors...")
        write_df_to_postgres(error_df, ERROR_TABLE)

        print("================================================")
        print(f"Transactions Pipeline completed successfully for {day}")
        print("Next step: run 040_upsert_transactions.sql")
        print("================================================")

    except Exception as e:
        print(f"Transactions Pipeline failed: {e}")
        raise

    finally:
        spark.stop()


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Transactions Pipeline")
    parser.add_argument("--day", required=True, help="Day folder to process (e.g. day1)")

    args = parser.parse_args()
    run_transactions_pipeline(args.day)
