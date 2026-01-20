import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, BooleanType, TimestampType
)

from src.pipelines.roles.roles_transform import transform_roles


# --------------------------------------------------
# Spark fixture
# --------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("roles-transform-test")
        .getOrCreate()
    )


# --------------------------------------------------
# FLAT SCHEMA
# --------------------------------------------------
@pytest.fixture
def flat_roles_schema():
    return StructType([
        StructField("id", StringType(), True),   # STRING to allow try_cast testing
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("default", BooleanType(), True),
        StructField("agent_type", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])


# --------------------------------------------------
# NESTED SCHEMA
# --------------------------------------------------
@pytest.fixture
def nested_roles_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField(
            "role",
            StructType([
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("default", BooleanType(), True),
                StructField("agent_type", StringType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
            ]),
            True
        ),
    ])


# ==================================================
# 1. VALID FLAT ROLE
# ==================================================
def test_transform_roles_valid_flat(spark, flat_roles_schema):
    df = spark.createDataFrame(
        [("1", "Admin", "Administrator role", True, "support", None, None)],
        flat_roles_schema
    )

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 1
    assert error_df.count() == 0

    row = final_df.collect()[0]
    assert row.role_id == 1
    assert row.name == "Admin"


# ==================================================
# 2. VALID NESTED ROLE (safe_nested_col)
# ==================================================
def test_transform_roles_valid_nested(spark, nested_roles_schema):
    df = spark.createDataFrame(
        [
            (
                "10",
                ("Supervisor", "Supervisory role", False, "admin", None, None)
            )
        ],
        nested_roles_schema
    )

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 1
    assert error_df.count() == 0

    row = final_df.collect()[0]
    assert row.role_id == 10
    assert row.name == "Supervisor"
    assert row.is_default is False


# ==================================================
# 3. MISSING NAME (nested)
# ==================================================
def test_transform_roles_missing_name_nested(spark, nested_roles_schema):
    df = spark.createDataFrame(
        [
            (
                "5",
                (None, "No name", True, "support", None, None)
            )
        ],
        nested_roles_schema
    )

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 0
    assert error_df.count() == 1


# ==================================================
# 4. NON-NUMERIC ID (try_cast path)
# ==================================================
def test_transform_roles_non_numeric_id(spark, flat_roles_schema):
    df = spark.createDataFrame(
        [("abc", "Admin", "Role", True, "support", None, None)],
        flat_roles_schema
    )

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 0
    assert error_df.count() == 1


# ==================================================
# 5. MIXED FLAT + NESTED (schema drift)
# ==================================================
def test_transform_roles_mixed_flat_nested(spark, flat_roles_schema, nested_roles_schema):
    flat_df = spark.createDataFrame(
        [("1", "Admin", "Administrator", True, "support", None, None)],
        flat_roles_schema
    )

    nested_df = spark.createDataFrame(
        [
            (
                "2",
                ("Agent", "Agent role", False, "support", None, None)
            )
        ],
        nested_roles_schema
    )

    df = flat_df.unionByName(nested_df, allowMissingColumns=True)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 2
    assert error_df.count() == 0


# ==================================================
# 6. EXTRA FIELDS (schema drift)
# ==================================================
def test_transform_roles_extra_fields_ignored(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("default", BooleanType(), True),
        StructField("agent_type", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("unexpected", StringType(), True),
    ])

    df = spark.createDataFrame(
        [("7", "Custom", "Extra field", True, "support", None, None, "boom")],
        schema
    )

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 1
    assert error_df.count() == 0


# ==================================================
# 7. EMPTY INPUT
# ==================================================
def test_transform_roles_empty_df(spark, flat_roles_schema):
    df = spark.createDataFrame([], flat_roles_schema)

    final_df, error_df = transform_roles(df)

    assert final_df.count() == 0
    assert error_df.count() == 0


# ==================================================
# 8. RAW RECORD CAPTURE
# ==================================================
def test_raw_record_present_in_error_df(spark, flat_roles_schema):
    df = spark.createDataFrame(
        [(None, None, "Bad role", False, "support", None, None)],
        flat_roles_schema
    )

    _, error_df = transform_roles(df)

    raw = error_df.collect()[0].raw_record
    assert raw is not None
    assert "Bad role" in raw
