import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, ArrayType
)
from src.pipelines.groups.groups_pipeline import transform_groups


# -----------------------------
# Spark Fixture
# -----------------------------
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("groups-pipeline-test") \
        .getOrCreate()


# -----------------------------
# Schemas
# -----------------------------
groups_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("parent_group_id", LongType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("agent_ids", ArrayType(LongType()), True),
])

agents_schema = StructType([
    StructField("agent_id", LongType(), True)
])


# -----------------------------
# Test 1: All Valid Membership
# -----------------------------
def test_all_valid_membership(spark):
    raw_data = [
        {
            "id": 1,
            "name": "Support Team",
            "parent_group_id": None,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "agent_ids": [10, 11]
        }
    ]

    agents_data = [
        {"agent_id": 10},
        {"agent_id": 11}
    ]

    raw_df = spark.createDataFrame(raw_data, schema=groups_schema)
    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert groups_df.count() == 1
    assert valid_df.count() == 2
    assert error_df.count() == 0


# -----------------------------
# Test 2: All Invalid Membership
# -----------------------------
def test_all_invalid_membership(spark):
    raw_data = [
        {
            "id": 2,
            "name": "Sales Team",
            "parent_group_id": None,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "agent_ids": [99, 100]
        }
    ]

    agents_data = [
        {"agent_id": 10}
    ]

    raw_df = spark.createDataFrame(raw_data, schema=groups_schema)
    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert groups_df.count() == 1
    assert valid_df.count() == 0
    assert error_df.count() == 2
    assert error_df.collect()[0]["error_type"] == "INVALID_AGENT_ID"


# -----------------------------
# Test 3: Mixed Valid + Invalid
# -----------------------------
def test_mixed_valid_and_invalid_membership(spark):
    raw_data = [
        {
            "id": 3,
            "name": "Engineering",
            "parent_group_id": None,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "agent_ids": [10, 999]
        }
    ]

    agents_data = [
        {"agent_id": 10}
    ]

    raw_df = spark.createDataFrame(raw_data, schema=groups_schema)
    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert valid_df.count() == 1
    assert error_df.count() == 1


# -----------------------------
# Test 4: Empty agent_ids
# -----------------------------
def test_empty_agent_ids(spark):
    raw_data = [
        {
            "id": 4,
            "name": "HR",
            "parent_group_id": None,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "agent_ids": []
        }
    ]

    agents_data = [
        {"agent_id": 1}
    ]

    raw_df = spark.createDataFrame(raw_data, schema=groups_schema)
    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert groups_df.count() == 1
    assert valid_df.count() == 0
    assert error_df.count() == 0


# -----------------------------
# Test 5: Null agent_ids
# -----------------------------
def test_null_agent_ids(spark):
    raw_data = [
        {
            "id": 5,
            "name": "Finance",
            "parent_group_id": None,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "agent_ids": None
        }
    ]

    agents_data = [
        {"agent_id": 1}
    ]

    raw_df = spark.createDataFrame(raw_data, schema=groups_schema)
    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert groups_df.count() == 1
    assert valid_df.count() == 0
    assert error_df.count() == 0
