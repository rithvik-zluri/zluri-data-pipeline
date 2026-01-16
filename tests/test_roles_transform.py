import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType, TimestampType
from src.pipelines.roles.roles_transform import transform_roles


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("roles-transform-test")
        .getOrCreate()
    )


@pytest.fixture
def roles_schema():
    return StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("default", BooleanType(), True),
        StructField("agent_type", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])


# --------------------------------------------------
# 1. VALID ROLE → should go to final_df, not error_df
# --------------------------------------------------
def test_transform_roles_valid(spark, roles_schema):
    data = [
        (1, "Admin", "Administrator role", True, "support", None, None)
    ]

    df = spark.createDataFrame(data, roles_schema)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 1
    assert error_df.count() == 0

    row = final_df.collect()[0]
    assert row.role_id == 1
    assert row.name == "Admin"
    assert row.is_default is True


# --------------------------------------------------
# 2. MISSING ID → should go to error_df
# --------------------------------------------------
def test_transform_roles_missing_id(spark, roles_schema):
    data = [
        (None, "Admin", "Administrator role", True, "support", None, None)
    ]

    df = spark.createDataFrame(data, roles_schema)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 0
    assert error_df.count() == 1

    error_row = error_df.collect()[0]
    assert error_row.error_type == "MISSING_REQUIRED_FIELD"
    assert "Role id or name is null" in error_row.error_message


# --------------------------------------------------
# 3. MISSING NAME → should go to error_df
# --------------------------------------------------
def test_transform_roles_missing_name(spark, roles_schema):
    data = [
        (1, None, "Administrator role", True, "support", None, None)
    ]

    df = spark.createDataFrame(data, roles_schema)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 0
    assert error_df.count() == 1

    error_row = error_df.collect()[0]
    assert error_row.role_id == 1
    assert error_row.error_type == "MISSING_REQUIRED_FIELD"


# --------------------------------------------------
# 4. BOTH ID AND NAME MISSING → error_df only
# --------------------------------------------------
def test_transform_roles_both_missing(spark, roles_schema):
    data = [
        (None, None, "No role", False, "support", None, None)
    ]

    df = spark.createDataFrame(data, roles_schema)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 0
    assert error_df.count() == 1


# --------------------------------------------------
# 5. MIXED DATA → one valid, one invalid
# --------------------------------------------------
def test_transform_roles_mixed_records(spark, roles_schema):
    data = [
        (1, "Admin", "Administrator role", True, "support", None, None),
        (None, "Agent", "Agent role", False, "support", None, None),
    ]

    df = spark.createDataFrame(data, roles_schema)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 1
    assert error_df.count() == 1

    final_row = final_df.collect()[0]
    assert final_row.role_id == 1
    assert final_row.name == "Admin"


# --------------------------------------------------
# 6. EMPTY INPUT → both outputs empty
# --------------------------------------------------
def test_transform_roles_empty_df(spark, roles_schema):
    df = spark.createDataFrame([], roles_schema)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 0
    assert error_df.count() == 0


# --------------------------------------------------
# 7. FIELD MAPPING TEST (aliasing + casting)
# --------------------------------------------------
def test_transform_roles_field_mapping(spark, roles_schema):
    data = [
        (10, "Supervisor", "Supervisory role", False, "admin", None, None)
    ]

    df = spark.createDataFrame(data, roles_schema)

    final_df, error_df = transform_roles(df)

    row = final_df.collect()[0]

    assert row.role_id == 10              # cast + alias
    assert row.name == "Supervisor"
    assert row.description == "Supervisory role"
    assert row.is_default is False        # alias from `default`
    assert row.agent_type == "admin"
