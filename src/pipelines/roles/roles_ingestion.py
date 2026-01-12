# src/pipelines/roles/roles_ingestion.py

import os
from src.utils.reader import DataReader

def read_roles(spark, day: str):
    """
    Reads roles data for a given sync day.
    """
    reader = DataReader(spark)
    input_path = os.path.join("sample_data", f"sync-{day}", "roles")
    print(f"Reading roles from: {input_path}")
    return reader.read(input_path, "json")
    