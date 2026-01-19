# src/pipelines/budgets/budgets_ingestion.py

import os
from pyspark.sql import DataFrame
from src.utils.reader import DataReader


def read_budgets_raw(reader: DataReader, day: str) -> DataFrame:
    """
    Reads budgets data for a given sync day.

    Works with:
    - Local paths (sample_data/...)
    - S3 paths (s3a://...)
    """

    base_data_path = os.environ.get("DATA_BASE_PATH", "sample_data")

    # Do NOT use os.path.join for S3 paths
    base_path = f"{base_data_path}/sync-{day}/budgets"

    print(f"Reading budgets from: {base_path}")

    return reader.read(base_path, "json")
