import pytest
from pyspark.sql import SparkSession
from src.pipelines.agents_pipeline import determine_agent_status

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-spark") \
        .getOrCreate()

def test_determine_agent_status(spark):
    # Input data
    all_agents_data = [
        ("1",),
        ("2",),
        ("3",)
    ]

    active_agents_data = [
        ("1", True),
        ("3", True)
    ]

    all_agents_df = spark.createDataFrame(all_agents_data, ["agent_id"])
    active_agents_df = spark.createDataFrame(active_agents_data, ["agent_id", "is_active"])

    # Call function
    result_df = determine_agent_status(all_agents_df, active_agents_df)

    result = {row["agent_id"]: row["status"] for row in result_df.collect()}

    # Assertions
    assert result["1"] == "active"
    assert result["3"] == "active"
    assert result["2"] == "inactive"
