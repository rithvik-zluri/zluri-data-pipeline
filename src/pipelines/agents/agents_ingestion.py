# src/pipelines/agents/agents_ingestion.py

import os
from src.utils.reader import DataReader


def read_agents_data(spark, day: str):
    """
    Reads agents and agent_details data for a given day.

    Works with:
    - Local paths (sample_data/...)
    - S3 paths (s3a://...)
    """

    # Base path can be local or S3
    base_data_path = os.environ.get("DATA_BASE_PATH", "sample_data")

    # IMPORTANT: do NOT use os.path.join for S3 paths
    base_path = f"{base_data_path}/sync-{day}"

    agent_details_path = f"{base_path}/agent_details"
    agents_path = f"{base_path}/agents"

    reader = DataReader(spark)

    # Per-agent JSON files (69010960305.json, etc.)
    agent_details_df = reader.read(agent_details_path, file_format="json")

    # Active agents list
    agents_df = reader.read(agents_path, file_format="json")

    return agent_details_df, agents_df
