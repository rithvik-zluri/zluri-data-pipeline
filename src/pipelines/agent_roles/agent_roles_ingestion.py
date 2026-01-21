# src/pipelines/agent_roles/agent_roles_ingestion.py

import os
from src.utils.reader import DataReader



def read_agent_roles_inputs(spark, day: str):
    """
    Reads agent_details inputs for agent_roles pipeline.
    The agent_details folder contains JSON files (one per agent) 
    that include the 'role_ids' key.
    """

    base_data_path = os.environ.get("DATA_BASE_PATH", "sample_data")

    agent_details_path = f"{base_data_path}/sync-{day}/agent_details"
    roles_path = f"{base_data_path}/sync-{day}/roles"

    reader = DataReader(spark)

    print(f"Reading agent details from: {agent_details_path}")
    agent_details_df = reader.read(agent_details_path, "json")

    print(f"Reading roles from: {roles_path}")
    roles_df = reader.read(roles_path, "json")

    return agent_details_df, roles_df

