# src/pipelines/cards/cards_ingestion.py

from pyspark.sql import DataFrame
from src.utils.reader import DataReader
import os


def read_cards_raw(reader: DataReader, day: str) -> DataFrame:
    """
    Reads cards data for a given sync day.

    Works with:
    - Local paths (sample_data/...)
    - S3 paths (s3a://...)
    """

    base_data_path = os.environ.get("DATA_BASE_PATH", "sample_data")

    # Avoid os.path.join for S3 paths
    base_path = f"{base_data_path}/sync-{day}/cards"

    print(f"Reading cards from: {base_path}")

    raw_df = reader.read(
        path=base_path,
        file_format="json",
        multiline=True
    )

    return raw_df
