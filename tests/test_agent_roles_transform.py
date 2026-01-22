import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, ArrayType
)

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
        StructField("role_ids", ArrayType(LongType()), True),
    ])


@pytest.fixture
def roles_schema():
    return StructType([
        StructField("id", LongType(), True),
    ])


# --------------------------------------------------
# 1. ALL ROLES VALID
# --------------------------------------------------
def test_transform_agent_roles_all_valid(spark, agents_schema, roles_schema):
    agents_data = [
        (1, [10, 20]),
    ]

    roles_data = [
        (10,),
        (20,),
    ]

    agents_df = spark.createDataFrame(agents_data, agents_schema)
    roles_df = spark.createDataFrame(roles_data, roles_schema)

    valid_df, error_df = transform_agent_roles(agents_df, roles_df)

    results = {(r.agent_id, r.role_id) for r in valid_df.collect()}
    assert results == {(1, 10), (1, 20)}
    assert error_df.count() == 0


# --------------------------------------------------
# 2. SOME INVALID ROLES
# --------------------------------------------------
def test_transform_agent_roles_partial_invalid(spark, agents_schema, roles_schema):
    agents_data = [
        (1, [10, 99]),
    ]

    roles_data = [
        (10,),
    ]

    agents_df = spark.createDataFrame(agents_data, agents_schema)
    roles_df = spark.createDataFrame(roles_data, roles_schema)

    valid_df, error_df = transform_agent_roles(agents_df, roles_df)

    assert {(r.agent_id, r.role_id) for r in valid_df.collect()} == {(1, 10)}
    assert error_df.count() == 1

    err = error_df.collect()[0]
    assert err.error_type == "INVALID_ROLE_ID"


# --------------------------------------------------
# 3. ALL INVALID ROLES
# --------------------------------------------------
def test_transform_agent_roles_all_invalid(spark, agents_schema, roles_schema):
    agents_data = [
        (1, [99, 100]),
    ]

    roles_df = spark.createDataFrame([], roles_schema)
    agents_df = spark.createDataFrame(agents_data, agents_schema)

    valid_df, error_df = transform_agent_roles(agents_df, roles_df)

    assert valid_df.count() == 0
    assert error_df.count() == 2


# --------------------------------------------------
# 4. MULTIPLE AGENTS
# --------------------------------------------------
def test_transform_agent_roles_multiple_agents(spark, agents_schema, roles_schema):
    agents_data = [
        (1, [10]),
        (2, [20, 30]),
    ]

    roles_data = [
        (10,),
        (30,),
    ]

    agents_df = spark.createDataFrame(agents_data, agents_schema)
    roles_df = spark.createDataFrame(roles_data, roles_schema)

    valid_df, error_df = transform_agent_roles(agents_df, roles_df)

    valid = {(r.agent_id, r.role_id) for r in valid_df.collect()}
    assert valid == {(1, 10), (2, 30)}
    assert error_df.count() == 1


# --------------------------------------------------
# 5. EMPTY AGENTS
# --------------------------------------------------
def test_transform_agent_roles_empty_agents(spark, agents_schema, roles_schema):
    agents_df = spark.createDataFrame([], agents_schema)
    roles_df = spark.createDataFrame([(10,)], roles_schema)

    valid_df, error_df = transform_agent_roles(agents_df, roles_df)

    assert valid_df.count() == 0
    assert error_df.count() == 0


# --------------------------------------------------
# 6. FIELD TYPES
# --------------------------------------------------
def test_transform_agent_roles_field_types(spark, agents_schema, roles_schema):
    agents_data = [(5, [50])]
    roles_data = [(50,)]

    agents_df = spark.createDataFrame(agents_data, agents_schema)
    roles_df = spark.createDataFrame(roles_data, roles_schema)

    valid_df, _ = transform_agent_roles(agents_df, roles_df)

    schema = valid_df.schema
    assert schema["agent_id"].dataType == LongType()
    assert schema["role_id"].dataType == LongType()
