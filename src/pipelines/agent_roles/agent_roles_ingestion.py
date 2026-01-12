# src/pipelines/agent_roles/agent_roles_ingestion.py

from src.utils.reader import DataReader

def read_agent_roles_inputs(spark, day: str):
    reader = DataReader(spark)

    agents_path = f"sample_data/sync-{day}/agents"
    roles_path = f"sample_data/sync-{day}/roles"

    print(f"Reading agents from: {agents_path}")
    agents_df = reader.read(agents_path, "json")

    print(f"Reading roles from: {roles_path}")
    roles_df = reader.read(roles_path, "json")

    return agents_df, roles_df
