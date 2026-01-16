import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, ArrayType, DoubleType, StructType
)
from src.pipelines.budgets.budgets_transform import transform_budgets


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("budgets-transform-test")
        .getOrCreate()
    )


@pytest.fixture
def budgets_schema():
    return StructType([
        StructField("results", ArrayType(
            StructType([
                StructField("id", StringType(), True),
                StructField("uuid", StringType(), True),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("retired", BooleanType(), True),
                StructField("startDate", StringType(), True),
                StructField("recurringInterval", StringType(), True),
                StructField("timezone", StringType(), True),
                StructField("currentPeriod", StructType([
                    StructField("limit", DoubleType(), True),
                    StructField("overspendBuffer", DoubleType(), True),
                    StructField("assigned", DoubleType(), True),
                    StructField("spent", StructType([
                        StructField("cleared", DoubleType(), True),
                        StructField("pending", DoubleType(), True),
                        StructField("total", DoubleType(), True),
                    ]), True),
                ]), True),
            ])
        ), True),
    ])


# --------------------------------------------------
# 1. HAPPY PATH – single valid budget
# --------------------------------------------------
def test_transform_budgets_valid_single(spark, budgets_schema):
    raw_data = [{
        "results": [{
            "id": "b1",
            "uuid": "u1",
            "name": "AWS",
            "description": "Cloud",
            "retired": False,
            "startDate": "2024-01-01",
            "recurringInterval": "monthly",
            "timezone": "UTC",
            "currentPeriod": {
                "limit": 1000.0,
                "overspendBuffer": 100.0,
                "assigned": 900.0,
                "spent": {
                    "cleared": 200.0,
                    "pending": 50.0,
                    "total": 250.0
                }
            }
        }]
    }]

    raw_df = spark.createDataFrame(raw_data, budgets_schema)

    valid_df, error_df = transform_budgets(raw_df, day="2024-01-10")

    assert valid_df.count() == 1
    assert error_df.count() == 0

    row = valid_df.collect()[0]
    assert row.budget_id == "b1"
    assert row.limit_amount == 1000.0
    assert row.sync_day == "2024-01-10"


# --------------------------------------------------
# 2. MULTIPLE BUDGETS
# --------------------------------------------------
def test_transform_budgets_multiple_results(spark, budgets_schema):
    raw_data = [{
        "results": [
            {
                "id": "b1",
                "uuid": "u1",
                "name": "AWS",
                "description": "Cloud",
                "retired": False,
                "startDate": "2024-01-01",
                "recurringInterval": "monthly",
                "timezone": "UTC",
                "currentPeriod": {
                    "limit": 1000.0,
                    "overspendBuffer": 100.0,
                    "assigned": 900.0,
                    "spent": {"cleared": 200.0, "pending": 50.0, "total": 250.0}
                }
            },
            {
                "id": "b2",
                "uuid": "u2",
                "name": "GCP",
                "description": "Infra",
                "retired": False,
                "startDate": "2024-02-01",
                "recurringInterval": "monthly",
                "timezone": "UTC",
                "currentPeriod": {
                    "limit": 2000.0,
                    "overspendBuffer": 200.0,
                    "assigned": 1800.0,
                    "spent": {"cleared": 300.0, "pending": 20.0, "total": 320.0}
                }
            }
        ]
    }]

    raw_df = spark.createDataFrame(raw_data, budgets_schema)
    valid_df, error_df = transform_budgets(raw_df, day="2024-01-10")

    assert valid_df.count() == 2
    assert error_df.count() == 0


# --------------------------------------------------
# 3. NULL budget_id → validation error
# --------------------------------------------------
def test_transform_budgets_null_budget_id(spark, budgets_schema):
    raw_data = [{
        "results": [{
            "id": None,
            "uuid": "u1",
            "name": "AWS",
            "description": "Cloud",
            "retired": False,
            "startDate": "2024-01-01",
            "recurringInterval": "monthly",
            "timezone": "UTC",
            "currentPeriod": {
                "limit": 1000.0,
                "overspendBuffer": 100.0,
                "assigned": 900.0,
                "spent": {"cleared": 200.0, "pending": 50.0, "total": 250.0}
            }
        }]
    }]

    raw_df = spark.createDataFrame(raw_data, budgets_schema)
    valid_df, error_df = transform_budgets(raw_df, day="2024-01-10")

    assert valid_df.count() == 0
    assert error_df.count() == 1

    err = error_df.collect()[0]
    assert err.error_type == "VALIDATION_ERROR"


# --------------------------------------------------
# 4. NULL limit_amount → validation error
# --------------------------------------------------
def test_transform_budgets_null_limit_amount(spark, budgets_schema):
    raw_data = [{
        "results": [{
            "id": "b1",
            "uuid": "u1",
            "name": "AWS",
            "description": "Cloud",
            "retired": False,
            "startDate": "2024-01-01",
            "recurringInterval": "monthly",
            "timezone": "UTC",
            "currentPeriod": {
                "limit": None,
                "overspendBuffer": 100.0,
                "assigned": 900.0,
                "spent": {"cleared": 200.0, "pending": 50.0, "total": 250.0}
            }
        }]
    }]

    raw_df = spark.createDataFrame(raw_data, budgets_schema)
    valid_df, error_df = transform_budgets(raw_df, day="2024-01-10")

    assert valid_df.count() == 0
    assert error_df.count() == 1


# --------------------------------------------------
# 5. MIXED VALID + INVALID
# --------------------------------------------------
def test_transform_budgets_mixed_valid_invalid(spark, budgets_schema):
    raw_data = [{
        "results": [
            {
                "id": "b1",
                "uuid": "u1",
                "name": "AWS",
                "description": "Cloud",
                "retired": False,
                "startDate": "2024-01-01",
                "recurringInterval": "monthly",
                "timezone": "UTC",
                "currentPeriod": {
                    "limit": 1000.0,
                    "overspendBuffer": 100.0,
                    "assigned": 900.0,
                    "spent": {"cleared": 200.0, "pending": 50.0, "total": 250.0}
                }
            },
            {
                "id": None,
                "uuid": "u2",
                "name": "GCP",
                "description": "Infra",
                "retired": False,
                "startDate": "2024-02-01",
                "recurringInterval": "monthly",
                "timezone": "UTC",
                "currentPeriod": {
                    "limit": 2000.0,
                    "overspendBuffer": 200.0,
                    "assigned": 1800.0,
                    "spent": {"cleared": 300.0, "pending": 20.0, "total": 320.0}
                }
            }
        ]
    }]

    raw_df = spark.createDataFrame(raw_data, budgets_schema)
    valid_df, error_df = transform_budgets(raw_df, day="2024-01-10")

    assert valid_df.count() == 1
    assert error_df.count() == 1


# --------------------------------------------------
# 6. DEDUPLICATION
# --------------------------------------------------
def test_transform_budgets_deduplication(spark, budgets_schema):
    raw_data = [{
        "results": [
            {
                "id": "b1",
                "uuid": "u1",
                "name": "AWS",
                "description": "Cloud",
                "retired": False,
                "startDate": "2024-01-01",
                "recurringInterval": "monthly",
                "timezone": "UTC",
                "currentPeriod": {
                    "limit": 1000.0,
                    "overspendBuffer": 100.0,
                    "assigned": 900.0,
                    "spent": {"cleared": 200.0, "pending": 50.0, "total": 250.0}
                }
            },
            {
                "id": "b1",
                "uuid": "u1-dup",
                "name": "AWS Duplicate",
                "description": "Cloud",
                "retired": False,
                "startDate": "2024-01-01",
                "recurringInterval": "monthly",
                "timezone": "UTC",
                "currentPeriod": {
                    "limit": 1000.0,
                    "overspendBuffer": 100.0,
                    "assigned": 900.0,
                    "spent": {"cleared": 200.0, "pending": 50.0, "total": 250.0}
                }
            }
        ]
    }]

    raw_df = spark.createDataFrame(raw_data, budgets_schema)
    valid_df, error_df = transform_budgets(raw_df, day="2024-01-10")

    assert valid_df.count() == 1
    assert error_df.count() == 0


# --------------------------------------------------
# 7. EMPTY RESULTS ARRAY
# --------------------------------------------------
def test_transform_budgets_empty_results(spark, budgets_schema):
    raw_data = [{"results": []}]
    raw_df = spark.createDataFrame(raw_data, budgets_schema)

    valid_df, error_df = transform_budgets(raw_df, day="2024-01-10")

    assert valid_df.count() == 0
    assert error_df.count() == 0


# --------------------------------------------------
# 8. NULL RESULTS FIELD
# --------------------------------------------------
def test_transform_budgets_null_results(spark, budgets_schema):
    raw_data = [{"results": None}]
    raw_df = spark.createDataFrame(raw_data, budgets_schema)

    valid_df, error_df = transform_budgets(raw_df, day="2024-01-10")

    assert valid_df.count() == 0
    assert error_df.count() == 0


# --------------------------------------------------
# 9. DATE PARSING
# --------------------------------------------------
def test_transform_budgets_start_date_parsing(spark, budgets_schema):
    raw_data = [{
        "results": [{
            "id": "b1",
            "uuid": "u1",
            "name": "AWS",
            "description": "Cloud",
            "retired": False,
            "startDate": "2024-05-15",
            "recurringInterval": "monthly",
            "timezone": "UTC",
            "currentPeriod": {
                "limit": 1000.0,
                "overspendBuffer": 100.0,
                "assigned": 900.0,
                "spent": {"cleared": 200.0, "pending": 50.0, "total": 250.0}
            }
        }]
    }]

    raw_df = spark.createDataFrame(raw_data, budgets_schema)
    valid_df, _ = transform_budgets(raw_df, day="2024-01-10")

    row = valid_df.collect()[0]
    assert str(row.start_date) == "2024-05-15"


# --------------------------------------------------
# 10. SCHEMA VALIDATION
# --------------------------------------------------
def test_transform_budgets_schema(spark, budgets_schema):
    raw_data = [{
        "results": [{
            "id": "b1",
            "uuid": "u1",
            "name": "AWS",
            "description": "Cloud",
            "retired": False,
            "startDate": "2024-01-01",
            "recurringInterval": "monthly",
            "timezone": "UTC",
            "currentPeriod": {
                "limit": 1000.0,
                "overspendBuffer": 100.0,
                "assigned": 900.0,
                "spent": {"cleared": 200.0, "pending": 50.0, "total": 250.0}
            }
        }]
    }]

    raw_df = spark.createDataFrame(raw_data, budgets_schema)
    valid_df, error_df = transform_budgets(raw_df, day="2024-01-10")

    expected_columns = {
        "budget_id", "budget_uuid", "name", "description", "retired",
        "start_date", "recurring_interval", "timezone",
        "limit_amount", "overspend_buffer", "assigned_amount",
        "spent_cleared", "spent_pending", "spent_total",
        "sync_day", "source", "ingested_at"
    }

    assert expected_columns.issubset(set(valid_df.columns))
    assert "error_type" in error_df.columns
    assert "raw_record" in error_df.columns
