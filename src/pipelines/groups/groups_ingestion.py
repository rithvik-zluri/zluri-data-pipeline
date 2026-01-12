# src/pipelines/groups/groups_ingestion.py

import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.utils.reader import DataReader


def read_groups_raw(reader: DataReader, day: str) -> DataFrame:
    base_path = os.path.join("sample_data", f"sync-{day}", "admin_groups")
    print(f"Reading groups from: {base_path}")
    return reader.read(base_path, "json")


def read_agents_from_db(spark, jdbc_url: str, db_props: dict) -> DataFrame:
    agents_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "agents") \
        .option("user", db_props["user"]) \
        .option("password", db_props["password"]) \
        .option("driver", db_props["driver"]) \
        .load() \
        .select(col("agent_id").cast("bigint").alias("agent_id"))

    return agents_df
