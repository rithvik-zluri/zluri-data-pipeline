# src/pipelines/cards/cards_ingestion.py

from pyspark.sql import DataFrame
from src.utils.reader import DataReader
import os


def read_cards_raw(reader: DataReader, day: str) -> DataFrame:
    base_path = os.path.join("sample_data", f"sync-{day}", "cards")

    print(f"Reading cards from: {base_path}")

    raw_df = reader.read(
        path=base_path,
        file_format="json",
        multiline=True
    )

    return raw_df
