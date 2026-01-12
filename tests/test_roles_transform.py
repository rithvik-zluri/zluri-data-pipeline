import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType, IntegerType
from src.pipelines.roles.roles_pipeline import transform_roles


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("roles-pipeline-test") \
        .getOrCreate()


# -----------------------------
# Schema
# -----------------------------
roles_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("default", BooleanType(), True),
    StructField("agent_type", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])


# -----------------------------
# Test 1: Valid Role
# -----------------------------
def test_valid_role(spark):
    data = [
        {
            "id": 1,
            "name": "Admin",
            "description": "Admin role",
            "default": True,
            "agent_type": 1,
            "created_at": "2024-01-01",
            "updated_at": "2024-01-02"
        }
    ]

    df = spark.createDataFrame(data, schema=roles_schema)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 1
    assert error_df.count() == 0
    assert final_df.collect()[0]["role_id"] == 1


# -----------------------------
# Test 2: Missing ID → Error
# -----------------------------
def test_missing_id_goes_to_error(spark):
    data = [
        {
            "id": None,
            "name": "Support",
            "description": "Support role",
            "default": False,
            "agent_type": 2,
            "created_at": None,
            "updated_at": None
        }
    ]

    df = spark.createDataFrame(data, schema=roles_schema)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 0
    assert error_df.count() == 1
    assert error_df.collect()[0]["error_type"] == "MISSING_REQUIRED_FIELD"


# -----------------------------
# Test 3: Missing Name → Error
# -----------------------------
def test_missing_name_goes_to_error(spark):
    data = [
        {
            "id": 2,
            "name": None,
            "description": "No name role",
            "default": False,
            "agent_type": 2,
            "created_at": None,
            "updated_at": None
        }
    ]

    df = spark.createDataFrame(data, schema=roles_schema)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 0
    assert error_df.count() == 1


# -----------------------------
# Test 4: Mixed Valid + Invalid
# -----------------------------
def test_mixed_valid_and_invalid_roles(spark):
    data = [
        {
            "id": 3,
            "name": "Manager",
            "description": "Manager role",
            "default": False,
            "agent_type": 1,
            "created_at": None,
            "updated_at": None
        },
        {
            "id": None,
            "name": None,
            "description": "Broken role",
            "default": None,
            "agent_type": None,
            "created_at": None,
            "updated_at": None
        }
    ]

    df = spark.createDataFrame(data, schema=roles_schema)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 1
    assert error_df.count() == 1
    assert final_df.collect()[0]["role_id"] == 3


# -----------------------------
# Test 5: Edge Case – Duplicate Roles
# -----------------------------
def test_duplicate_roles_allowed(spark):
    data = [
        {"id": 4, "name": "QA", "description": None, "default": False, "agent_type": 1, "created_at": None, "updated_at": None},
        {"id": 4, "name": "QA", "description": None, "default": False, "agent_type": 1, "created_at": None, "updated_at": None},
    ]

    df = spark.createDataFrame(data, schema=roles_schema)

    final_df, error_df = transform_roles(df)

    # No dedup logic yet – both should pass
    assert final_df.count() == 2
    assert error_df.count() == 0
