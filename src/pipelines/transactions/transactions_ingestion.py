# src/pipelines/transactions/transactions_ingestion.py

import os
from pyspark.sql import SparkSession


def read_transactions(spark: SparkSession, day: str):
    """
    Reads transactions data for a given sync day.

    Works with:
    - Local paths (sample_data/...)
    - S3 paths (s3a://...)
    """

    base_data_path = os.environ.get("DATA_BASE_PATH", "sample_data")

    # Avoid os.path.join so s3a:// paths work correctly
    base_path = f"{base_data_path}/sync-{day}/transactions"

    print(f"Reading transactions from: {base_path}")

    df = (
        spark.read
        .option("multiline", "true")
        .json(base_path)
    )

    return df
