import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType
)

from src.pipelines.agents.agents_transform import transform_agents


# ------------------------------------------------------
# Spark Fixture
# ------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("agents-transform-tests") \
        .getOrCreate()


# ------------------------------------------------------
# Schemas
# ------------------------------------------------------
contact_schema = StructType([
    StructField("email", StringType(), True),
    StructField("name", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("language", StringType(), True),
    StructField("mobile", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("time_zone", StringType(), True),
])

agent_details_schema = StructType([
    StructField("id", LongType(), True),
    StructField("contact", contact_schema, True),
    StructField("available", BooleanType(), True),
    StructField("deactivated", BooleanType(), True),
    StructField("focus_mode", BooleanType(), True),
    StructField("agent_operational_status", StringType(), True),
    StructField("last_active_at", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])


# ------------------------------------------------------
# Helpers
# ------------------------------------------------------
def get_valid_agent_details_df(spark):
    data = [
        Row(
            id=1,
            contact=Row(
                email="john@example.com",
                name="John Doe",
                job_title="Support",
                language="en",
                mobile="123",
                phone="456",
                time_zone="UTC"
            ),
            available=True,
            deactivated=False,
            focus_mode=False,
            agent_operational_status="online",
            last_active_at="2024-01-01T10:00:00",
            created_at="2023-01-01T10:00:00",
            updated_at="2024-01-01T10:00:00"
        )
    ]
    return spark.createDataFrame(data, schema=agent_details_schema)


def get_agents_df(spark, ids):
    rows = [Row(id=i) for i in ids]
    return spark.createDataFrame(rows, schema=StructType([
        StructField("id", LongType(), True)
    ]))


# ======================================================
# TEST 1 – Valid record → active
# ======================================================
def test_transform_agents_valid_active(spark):
    agent_details_df = get_valid_agent_details_df(spark)
    agents_df = get_agents_df(spark, [1])

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    result = final_df.collect()
    errors = error_df.collect()

    assert len(result) == 1
    row = result[0]

    assert row["agent_id"] == 1
    assert row["email"] == "john@example.com"
    assert row["name"] == "John Doe"
    assert row["status"] == "active"
    assert len(errors) == 0


# ======================================================
# TEST 2 – Missing required field → error_df
# ======================================================
def test_transform_agents_missing_required_field(spark):
    data = [
        Row(
            id=2,
            contact=Row(
                email=None,   # Missing required
                name="Jane Doe",
                job_title=None,
                language=None,
                mobile=None,
                phone=None,
                time_zone=None
            ),
            available=None,
            deactivated=None,
            focus_mode=None,
            agent_operational_status=None,
            last_active_at=None,
            created_at=None,
            updated_at=None
        )
    ]

    agent_details_df = spark.createDataFrame(data, schema=agent_details_schema)
    agents_df = get_agents_df(spark, [])

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    result = final_df.collect()
    errors = error_df.collect()

    assert len(result) == 0
    assert len(errors) == 1
    assert errors[0]["agent_id"] == 2
    assert errors[0]["error_type"] == "MISSING_REQUIRED_FIELD"


# ======================================================
# TEST 3 – Optional columns missing → auto added
# ======================================================
def test_transform_agents_optional_columns_added(spark):
    data = [
        Row(
            id=3,
            contact=Row(
                email="opt@example.com",
                name="Optional Test",
                job_title=None,
                language=None,
                mobile=None,
                phone=None,
                time_zone=None
            ),
            available=None,
            deactivated=None,
            focus_mode=None,
            agent_operational_status=None,
            last_active_at=None,
            created_at=None,
            updated_at=None
        )
    ]

    agent_details_df = spark.createDataFrame(data, schema=agent_details_schema)
    agents_df = get_agents_df(spark, [3])

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    row = final_df.collect()[0]

    # Columns must exist
    assert "available" in final_df.columns
    assert "deactivated" in final_df.columns
    assert "focus_mode" in final_df.columns
    assert "agent_operational_status" in final_df.columns
    assert "last_active_at" in final_df.columns
    assert "created_at" in final_df.columns
    assert "updated_at" in final_df.columns

    assert row["status"] == "active"
    assert len(error_df.collect()) == 0


# ======================================================
# TEST 4 – Not in agents_df → inactive
# ======================================================
def test_transform_agents_inactive_status(spark):
    agent_details_df = get_valid_agent_details_df(spark)
    agents_df = get_agents_df(spark, [999])  # different id

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    row = final_df.collect()[0]

    assert row["agent_id"] == 1
    assert row["status"] == "inactive"
    assert len(error_df.collect()) == 0


# ======================================================
# TEST 5 – Empty agents_df branch
# ======================================================
def test_transform_agents_empty_agents_df(spark):
    agent_details_df = get_valid_agent_details_df(spark)
    agents_df = get_agents_df(spark, [])

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    row = final_df.collect()[0]

    assert row["agent_id"] == 1
    assert row["status"] == "inactive"
    assert len(error_df.collect()) == 0


# ======================================================
# TEST 6 – Multiple records, mixed valid & invalid
# ======================================================
def test_transform_agents_mixed_records(spark):
    data = [
        # Valid
        Row(
            id=10,
            contact=Row(
                email="a@test.com",
                name="A",
                job_title=None,
                language=None,
                mobile=None,
                phone=None,
                time_zone=None
            ),
            available=None,
            deactivated=None,
            focus_mode=None,
            agent_operational_status=None,
            last_active_at=None,
            created_at=None,
            updated_at=None
        ),
        # Invalid
        Row(
            id=11,
            contact=Row(
                email=None,
                name=None,
                job_title=None,
                language=None,
                mobile=None,
                phone=None,
                time_zone=None
            ),
            available=None,
            deactivated=None,
            focus_mode=None,
            agent_operational_status=None,
            last_active_at=None,
            created_at=None,
            updated_at=None
        )
    ]

    agent_details_df = spark.createDataFrame(data, schema=agent_details_schema)
    agents_df = get_agents_df(spark, [10])

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    assert final_df.count() == 1
    assert error_df.count() == 1

    row = final_df.collect()[0]
    assert row["agent_id"] == 10
    assert row["status"] == "active"
