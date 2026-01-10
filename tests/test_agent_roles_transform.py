from pyspark.sql import SparkSession
from src.pipelines.agent_roles_pipeline import build_agent_role_mapping
import pytest

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

def test_agent_role_mapping(spark):
    agent_details_data = [
        ("1", [ {"id": "10"}, {"id": "20"} ]),
        ("2", [ {"id": "10"} ])
    ]

    df = spark.createDataFrame(agent_details_data, ["agent_id", "roles"])

    result_df = build_agent_role_mapping(df)

    result = [(row.agent_id, row.role_id) for row in result_df.collect()]

    assert ("1", "10") in result
    assert ("1", "20") in result
    assert ("2", "10") in result
