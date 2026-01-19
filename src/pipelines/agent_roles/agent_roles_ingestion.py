# src/pipelines/agent_roles/agent_roles_ingestion.py

import os
from src.utils.reader import DataReader


def read_agent_roles_inputs(spark, day: str):
    """
    Reads agents and roles inputs for agent_roles pipeline.

    Works with:
    - Local paths (sample_data/...)
    - S3 paths (s3a://...)
    """

    base_data_path = os.environ.get("DATA_BASE_PATH", "sample_data")

    # Use string formatting instead of os.path.join for S3 safety
    agents_path = f"{base_data_path}/sync-{day}/agents"
    roles_path = f"{base_data_path}/sync-{day}/roles"

    reader = DataReader(spark)

    print(f"Reading agents from: {agents_path}")
    agents_df = reader.read(agents_path, "json")

    print(f"Reading roles from: {roles_path}")
    roles_df = reader.read(roles_path, "json")

    return agents_df, roles_df
