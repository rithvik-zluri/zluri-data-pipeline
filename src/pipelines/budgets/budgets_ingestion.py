import os
from pyspark.sql import DataFrame
from src.utils.reader import DataReader

def read_budgets_raw(reader: DataReader, day: str) -> DataFrame:
    base_path = os.path.join("sample_data", f"sync-{day}", "budgets")
    print(f"Reading budgets from: {base_path}")
    return reader.read(base_path, "json")
