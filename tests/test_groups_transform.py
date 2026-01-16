import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, ArrayType, IntegerType
)
from src.pipelines.groups.groups_transform import transform_groups


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("groups-transform-test")
        .getOrCreate()
    )


@pytest.fixture
def groups_schema():
    return StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("parent_group_id", LongType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("agent_ids", ArrayType(LongType()), True),
    ])


@pytest.fixture
def agents_schema():
    return StructType([
        StructField("agent_id", LongType(), True),
        StructField("name", StringType(), True),
    ])


# --------------------------------------------------
# 1. HAPPY PATH – all agents valid
# --------------------------------------------------
def test_transform_groups_all_valid_agents(spark, groups_schema, agents_schema):
    raw_data = [
        (1, "Support", None, "2024-01-01", "2024-01-02", [10, 20])
    ]

    agents_data = [
        (10, "Alice"),
        (20, "Bob"),
    ]

    raw_df = spark.createDataFrame(raw_data, groups_schema)
    agents_df = spark.createDataFrame(agents_data, agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert groups_df.count() == 1
    assert valid_df.count() == 2
    assert error_df.count() == 0

    group_row = groups_df.collect()[0]
    assert group_row.group_id == 1
    assert group_row.name == "Support"


# --------------------------------------------------
# 2. MIXED VALID + INVALID AGENTS
# --------------------------------------------------
def test_transform_groups_mixed_valid_invalid_agents(spark, groups_schema, agents_schema):
    raw_data = [
        (1, "IT", None, "2024-01-01", "2024-01-02", [10, 99])
    ]

    agents_data = [
        (10, "Alice"),
    ]

    raw_df = spark.createDataFrame(raw_data, groups_schema)
    agents_df = spark.createDataFrame(agents_data, agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert valid_df.count() == 1
    assert error_df.count() == 1

    error_row = error_df.collect()[0]
    assert error_row.group_id == 1
    assert error_row.error_type == "INVALID_AGENT_ID"


# --------------------------------------------------
# 3. ALL INVALID AGENTS
# --------------------------------------------------
def test_transform_groups_all_invalid_agents(spark, groups_schema, agents_schema):
    raw_data = [
        (2, "HR", None, "2024-01-01", "2024-01-02", [50, 60])
    ]

    agents_data = [
        (10, "Alice"),
    ]

    raw_df = spark.createDataFrame(raw_data, groups_schema)
    agents_df = spark.createDataFrame(agents_data, agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert valid_df.count() == 0
    assert error_df.count() == 2


# --------------------------------------------------
# 4. EMPTY agent_ids ARRAY
# --------------------------------------------------
def test_transform_groups_empty_agent_ids(spark, groups_schema, agents_schema):
    raw_data = [
        (3, "Finance", None, "2024-01-01", "2024-01-02", [])
    ]

    agents_data = [
        (10, "Alice"),
    ]

    raw_df = spark.createDataFrame(raw_data, groups_schema)
    agents_df = spark.createDataFrame(agents_data, agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert groups_df.count() == 1
    assert valid_df.count() == 0
    assert error_df.count() == 0


# --------------------------------------------------
# 5. NULL agent_ids
# --------------------------------------------------
def test_transform_groups_null_agent_ids(spark, groups_schema, agents_schema):
    raw_data = [
        (4, "Legal", None, "2024-01-01", "2024-01-02", None)
    ]

    agents_data = [
        (10, "Alice"),
    ]

    raw_df = spark.createDataFrame(raw_data, groups_schema)
    agents_df = spark.createDataFrame(agents_data, agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert groups_df.count() == 1
    assert valid_df.count() == 0
    assert error_df.count() == 0


# --------------------------------------------------
# 6. EMPTY INPUT DATAFRAME
# --------------------------------------------------
def test_transform_groups_empty_raw_df(spark, groups_schema, agents_schema):
    raw_df = spark.createDataFrame([], groups_schema)

    agents_data = [
        (10, "Alice"),
    ]
    agents_df = spark.createDataFrame(agents_data, agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert groups_df.count() == 0
    assert valid_df.count() == 0
    assert error_df.count() == 0


# --------------------------------------------------
# 7. PARENT GROUP HANDLING
# --------------------------------------------------
def test_transform_groups_parent_group(spark, groups_schema, agents_schema):
    raw_data = [
        (10, "Parent", None, "2024-01-01", "2024-01-02", []),
        (11, "Child", 10, "2024-01-01", "2024-01-02", [])
    ]

    agents_data = []

    raw_df = spark.createDataFrame(raw_data, groups_schema)
    agents_df = spark.createDataFrame(agents_data, agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    rows = {row.group_id: row.parent_group_id for row in groups_df.collect()}

    assert rows[10] is None
    assert rows[11] == 10


# --------------------------------------------------
# 8. SCHEMA VALIDATION
# --------------------------------------------------
def test_transform_groups_schema(spark, groups_schema, agents_schema):
    raw_data = [
        (1, "Support", None, "2024-01-01", "2024-01-02", [10])
    ]

    agents_data = [
        (10, "Alice"),
    ]

    raw_df = spark.createDataFrame(raw_data, groups_schema)
    agents_df = spark.createDataFrame(agents_data, agents_schema)

    groups_df, valid_df, error_df = transform_groups(raw_df, agents_df)

    assert "group_id" in groups_df.columns
    assert "agent_id" in valid_df.columns
    assert "error_type" in error_df.columns
    assert "raw_record" in error_df.columns
