# src/pipelines/roles/roles_ingestion.py

import os
from src.utils.reader import DataReader


def read_roles(spark, day: str):
    """
    Reads roles data for a given sync day.

    Works with:
    - Local paths (sample_data/...)
    - S3 paths (s3a://...)
    """

    reader = DataReader(spark)

    # Base path can be local or S3
    base_data_path = os.environ.get("DATA_BASE_PATH", "sample_data")

    # IMPORTANT: do NOT use os.path.join for S3 paths
    input_path = f"{base_data_path}/sync-{day}/roles"

    print(f"Reading roles from: {input_path}")

    return reader.read(input_path, "json")
