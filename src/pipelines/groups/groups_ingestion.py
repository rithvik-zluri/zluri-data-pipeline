# src/pipelines/groups/groups_ingestion.py

import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.utils.reader import DataReader


def read_groups_raw(reader: DataReader, day: str) -> DataFrame:
    """
    Reads admin groups data for a given sync day.

    Works with:
    - Local paths (sample_data/...)
    - S3 paths (s3a://...)
    """

    # Base path can be local or S3
    base_data_path = os.environ.get("DATA_BASE_PATH", "sample_data")

    # IMPORTANT: do NOT use os.path.join for S3 paths
    base_path = f"{base_data_path}/sync-{day}/admin_groups"

    print(f"Reading groups from: {base_path}")

    return reader.read(base_path, "json")


def read_agents_from_db(spark, jdbc_url: str, db_props: dict) -> DataFrame:
    """
    Reads agent IDs from the database.
    (Unchanged – DB access is independent of local/S3)
    """

    agents_df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "agents")
        .option("user", db_props["user"])
        .option("password", db_props["password"])
        .option("driver", db_props["driver"])
        .load()
        .select(col("agent_id").cast("bigint").alias("agent_id"))
    )

    return agents_df
