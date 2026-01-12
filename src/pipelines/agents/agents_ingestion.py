# src/pipelines/agents/agents_ingestion.py

import os
from src.utils.reader import DataReader


def read_agents_data(spark, day: str):
    base_path = os.path.join("sample_data", f"sync-{day}")
    agent_details_path = os.path.join(base_path, "agent_details")
    agents_path = os.path.join(base_path, "agents")

    reader = DataReader(spark)

    agent_details_df = reader.read(agent_details_path, file_format="json")
    agents_df = reader.read(agents_path, file_format="json")

    return agent_details_df, agents_df
