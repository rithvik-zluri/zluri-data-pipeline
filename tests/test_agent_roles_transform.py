import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from src.pipelines.agent_roles.agent_roles_transform import transform_agent_roles


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("agent-roles-transform-test")
        .getOrCreate()
    )


@pytest.fixture
def agents_schema():
    return StructType([
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
    ])


@pytest.fixture
def roles_schema():
    return StructType([
        StructField("id", LongType(), True),
        StructField("agent_type", IntegerType(), True),
    ])


# --------------------------------------------------
# 1. SUPPORT AGENT → agent_type = 1 → join success
# --------------------------------------------------
def test_transform_agent_roles_support_agent(spark, agents_schema, roles_schema):
    agents_data = [
        (1, "support_agent")
    ]

    roles_data = [
        (10, 1)
    ]

    agents_df = spark.createDataFrame(agents_data, agents_schema)
    roles_df = spark.createDataFrame(roles_data, roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)

    assert result_df.count() == 1

    row = result_df.collect()[0]
    assert row.agent_id == 1
    assert row.role_id == 10


# --------------------------------------------------
# 2. NON-SUPPORT AGENT → agent_type = 3 → join success
# --------------------------------------------------
def test_transform_agent_roles_non_support_agent(spark, agents_schema, roles_schema):
    agents_data = [
        (2, "collaborator")
    ]

    roles_data = [
        (20, 3)
    ]

    agents_df = spark.createDataFrame(agents_data, agents_schema)
    roles_df = spark.createDataFrame(roles_data, roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)

    assert result_df.count() == 1

    row = result_df.collect()[0]
    assert row.agent_id == 2
    assert row.role_id == 20


# --------------------------------------------------
# 3. NO MATCH → result empty
# --------------------------------------------------
def test_transform_agent_roles_no_match(spark, agents_schema, roles_schema):
    agents_data = [
        (1, "support_agent")
    ]

    roles_data = [
        (10, 3)  # mismatch
    ]

    agents_df = spark.createDataFrame(agents_data, agents_schema)
    roles_df = spark.createDataFrame(roles_data, roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)

    assert result_df.count() == 0


# --------------------------------------------------
# 4. MIXED AGENTS → only matching join returned
# --------------------------------------------------
def test_transform_agent_roles_mixed_agents(spark, agents_schema, roles_schema):
    agents_data = [
        (1, "support_agent"),
        (2, "collaborator"),
        (3, "support_agent"),
    ]

    roles_data = [
        (10, 1),
        (20, 3),
    ]

    agents_df = spark.createDataFrame(agents_data, agents_schema)
    roles_df = spark.createDataFrame(roles_data, roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)

    results = {(row.agent_id, row.role_id) for row in result_df.collect()}

    assert results == {(1, 10), (2, 20), (3, 10)}


# --------------------------------------------------
# 5. EMPTY AGENTS → empty result
# --------------------------------------------------
def test_transform_agent_roles_empty_agents(spark, agents_schema, roles_schema):
    agents_df = spark.createDataFrame([], agents_schema)

    roles_data = [
        (10, 1),
        (20, 3),
    ]
    roles_df = spark.createDataFrame(roles_data, roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)

    assert result_df.count() == 0


# --------------------------------------------------
# 6. EMPTY ROLES → empty result
# --------------------------------------------------
def test_transform_agent_roles_empty_roles(spark, agents_schema, roles_schema):
    agents_data = [
        (1, "support_agent"),
        (2, "collaborator"),
    ]
    agents_df = spark.createDataFrame(agents_data, agents_schema)

    roles_df = spark.createDataFrame([], roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)

    assert result_df.count() == 0


# --------------------------------------------------
# 7. FIELD CASTING + ALIAS VALIDATION
# --------------------------------------------------
def test_transform_agent_roles_field_types(spark, agents_schema, roles_schema):
    agents_data = [
        (5, "support_agent")
    ]

    roles_data = [
        (50, 1)
    ]

    agents_df = spark.createDataFrame(agents_data, agents_schema)
    roles_df = spark.createDataFrame(roles_data, roles_schema)

    result_df = transform_agent_roles(agents_df, roles_df)

    schema = result_df.schema

    assert schema["agent_id"].dataType == LongType()
    assert schema["role_id"].dataType == LongType()
