import argparse

from pyspark.sql.functions import col

from src.db.connection import get_postgres_properties
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader
from src.pipelines.cards.cards_ingestion import read_cards_raw
from src.pipelines.cards.cards_transform import transform_cards

def run_cards_pipeline(day: str):
    print(f"=== Starting cards pipeline for {day} ===")

    spark = get_spark_session("cards-pipeline")
    reader = DataReader(spark)

    db_properties = get_postgres_properties()
    jdbc_url = db_properties["url"]

    # ---------------------------
    # INGESTION
    # ---------------------------
    raw_df = read_cards_raw(reader, day)

    # ---------------------------
    # TRANSFORM
    # ---------------------------
    valid_df, error_df = transform_cards(raw_df, day)

    # ---------------------------
    # IDEMPOTENCY (filter existing sync_day)
    # ---------------------------
    try:
        existing_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "stg_cards") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .load()

        valid_df = valid_df.join(
            existing_df.filter(col("sync_day") == day).select("card_id"),
            on="card_id",
            how="left_anti"
        )

        print("Idempotency filter applied")

    except Exception:
        print("stg_cards does not exist yet, skipping idempotency filter")

    # ---------------------------
    # FINAL DEDUP (CRITICAL)
    # ---------------------------
    valid_df = valid_df.dropDuplicates(["card_id"])
    
    # ---------------------------
    # WRITE STAGING
    # ---------------------------
    valid_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "stg_cards") \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .mode("append") \
        .save()

    print("Data appended to stg_cards")

    # ---------------------------
    # WRITE ERRORS
    # ---------------------------
    if not error_df.rdd.isEmpty():
        error_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "card_pipeline_errors") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .option("stringtype", "unspecified") \
            .mode("append") \
            .save()

        print("Error records written")

    print(f"=== Cards pipeline completed for {day} ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day folder, e.g. day1, day2")
    args = parser.parse_args()

    run_cards_pipeline(args.day)
