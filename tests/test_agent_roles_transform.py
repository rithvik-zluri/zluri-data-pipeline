import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from src.pipelines.agent_roles_pipeline import transform_agent_roles


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("agent-roles-pipeline-test") \
        .getOrCreate()


# -----------------------------
# Schemas
# -----------------------------

agents_schema = StructType([
    StructField("id", LongType(), True),
    StructField("type", StringType(), True)
])

roles_schema = StructType([
    StructField("id", LongType(), True),
    StructField("agent_type", IntegerType(), True)
])


# -----------------------------
# Test 1: Support Agent → Role Match
# -----------------------------
def test_support_agent_role_mapping(spark):
    agents_data = [
        {"id": 1, "type": "support_agent"}
    ]

    roles_data = [
        {"id": 10, "agent_type": 1}
    ]

    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)
    roles_df = spark.createDataFrame(roles_data, schema=roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)
    rows = result_df.collect()

    assert len(rows) == 1
    assert rows[0]["agent_id"] == 1
    assert rows[0]["role_id"] == 10


# -----------------------------
# Test 2: Non-Support Agent → Collaborator Mapping
# -----------------------------
def test_collaborator_agent_role_mapping(spark):
    agents_data = [
        {"id": 2, "type": "collaborator"}
    ]

    roles_data = [
        {"id": 20, "agent_type": 3}
    ]

    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)
    roles_df = spark.createDataFrame(roles_data, schema=roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)
    rows = result_df.collect()

    assert len(rows) == 1
    assert rows[0]["agent_id"] == 2
    assert rows[0]["role_id"] == 20


# -----------------------------
# Test 3: No Matching Role
# -----------------------------
def test_no_matching_role(spark):
    agents_data = [
        {"id": 3, "type": "support_agent"}
    ]

    roles_data = [
        {"id": 30, "agent_type": 3}  # mismatch
    ]

    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)
    roles_df = spark.createDataFrame(roles_data, schema=roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)

    assert result_df.count() == 0


# -----------------------------
# Test 4: Multiple Agents + Roles
# -----------------------------
def test_multiple_agents_and_roles(spark):
    agents_data = [
        {"id": 1, "type": "support_agent"},
        {"id": 2, "type": "collaborator"}
    ]

    roles_data = [
        {"id": 10, "agent_type": 1},
        {"id": 20, "agent_type": 3}
    ]

    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)
    roles_df = spark.createDataFrame(roles_data, schema=roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)
    rows = result_df.collect()

    assert len(rows) == 2

    mapping = {(r["agent_id"], r["role_id"]) for r in rows}

    assert (1, 10) in mapping
    assert (2, 20) in mapping


# -----------------------------
# Test 5: Empty Agents
# -----------------------------
def test_empty_agents(spark):
    agents_df = spark.createDataFrame([], schema=agents_schema)

    roles_data = [
        {"id": 10, "agent_type": 1}
    ]
    roles_df = spark.createDataFrame(roles_data, schema=roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)

    assert result_df.count() == 0


# -----------------------------
# Test 6: Empty Roles
# -----------------------------
def test_empty_roles(spark):
    agents_data = [
        {"id": 1, "type": "support_agent"}
    ]
    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)

    roles_df = spark.createDataFrame([], schema=roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)

    assert result_df.count() == 0
