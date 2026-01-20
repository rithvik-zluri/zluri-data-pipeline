import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)
from src.pipelines.cards.cards_transform import transform_cards


# =========================================================
# SPARK FIXTURE
# =========================================================
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("cards-transform-tests") \
        .getOrCreate()


# =========================================================
# NESTED INPUT FACTORY (existing)
# =========================================================
def create_raw_cards_df(spark, records):
    schema = StructType([
        StructField("results", ArrayType(
            StructType([
                StructField("id", StringType(), True),
                StructField("uuid", StringType(), True),
                StructField("name", StringType(), True),

                StructField("userId", StringType(), True),
                StructField("userUuid", StringType(), True),

                StructField("budgetId", StringType(), True),
                StructField("budgetUuid", StringType(), True),

                StructField("lastFour", StringType(), True),
                StructField("validThru", StringType(), True),

                StructField("status", StringType(), True),
                StructField("type", StringType(), True),

                StructField("shareBudgetFunds", StringType(), True),
                StructField("recurring", StringType(), True),
                StructField("recurringLimit", DoubleType(), True),

                StructField("currentPeriod", StructType([
                    StructField("limit", DoubleType(), True),
                    StructField("spent", DoubleType(), True),
                ])),

                StructField("createdTime", StringType(), True),
                StructField("updatedTime", StringType(), True),
            ])
        ), True)
    ])

    return spark.createDataFrame([{"results": records}], schema=schema)


# =========================================================
# FLAT INPUT FACTORY (NEW)
# =========================================================
def create_flat_cards_df(spark, records):
    return spark.createDataFrame(records)


# =========================================================
# 1. HAPPY PATH (NESTED)
# =========================================================
def test_transform_cards_valid_record(spark):
    raw_df = create_raw_cards_df(spark, [{
        "id": "c1",
        "uuid": "u1",
        "name": "Card 1",
        "userId": "user1",
        "userUuid": "uu1",
        "budgetId": "b1",
        "budgetUuid": "bu1",
        "lastFour": "1234",
        "validThru": "12/25",
        "status": "active",
        "type": "virtual",
        "shareBudgetFunds": "true",
        "recurring": "false",
        "recurringLimit": 100.0,
        "currentPeriod": {"limit": 500.0, "spent": 200.0},
        "createdTime": "2024-01-01T10:00:00",
        "updatedTime": "2024-01-02T10:00:00"
    }])

    valid_df, error_df = transform_cards(raw_df, "day1")

    assert valid_df.count() == 1
    assert error_df.count() == 0


# =========================================================
# 2. NULL MANDATORY FIELD → ERROR
# =========================================================
def test_transform_cards_null_card_id(spark):
    raw_df = create_raw_cards_df(spark, [{
        "id": None,
        "status": "active",
        "type": "virtual",
        "createdTime": "2024-01-01T10:00:00",
        "currentPeriod": {"limit": 100.0, "spent": 10.0}
    }])

    valid_df, error_df = transform_cards(raw_df, "day1")

    assert valid_df.count() == 0
    assert error_df.count() == 1


# =========================================================
# 3. NEGATIVE NUMBERS → ERROR
# =========================================================
def test_transform_cards_negative_values(spark):
    raw_df = create_raw_cards_df(spark, [{
        "id": "c2",
        "status": "active",
        "type": "physical",
        "recurringLimit": -10.0,
        "currentPeriod": {"limit": -100.0, "spent": -20.0},
        "createdTime": "2024-01-01T10:00:00"
    }])

    valid_df, error_df = transform_cards(raw_df, "day1")

    assert valid_df.count() == 0
    assert error_df.count() == 1


# =========================================================
# 4. NULL OPTIONAL NUMBERS → VALID
# =========================================================
def test_transform_cards_null_optional_numbers(spark):
    raw_df = create_raw_cards_df(spark, [{
        "id": "c3",
        "status": "inactive",
        "type": "virtual",
        "recurringLimit": None,
        "currentPeriod": {"limit": None, "spent": None},
        "createdTime": "2024-01-01T10:00:00"
    }])

    valid_df, error_df = transform_cards(raw_df, "day1")

    assert valid_df.count() == 1
    assert error_df.count() == 0


# =========================================================
# 5. DEDUPLICATION (NESTED)
# =========================================================
def test_transform_cards_deduplication(spark):
    raw_df = create_raw_cards_df(spark, [
        {
            "id": "c4",
            "status": "active",
            "type": "virtual",
            "currentPeriod": {"limit": 100.0, "spent": 10.0},
            "createdTime": "2024-01-01T10:00:00"
        },
        {
            "id": "c4",
            "status": "active",
            "type": "virtual",
            "currentPeriod": {"limit": 100.0, "spent": 10.0},
            "createdTime": "2024-01-01T10:00:00"
        }
    ])

    valid_df, error_df = transform_cards(raw_df, "day1")

    assert valid_df.count() == 1
    assert error_df.count() == 0


# =========================================================
# 6. FLAT INPUT – HAPPY PATH
# =========================================================
def test_transform_cards_flat_input_valid(spark):
    raw_df = create_flat_cards_df(spark, [{
        "id": "cf1",
        "status": "active",
        "type": "virtual",
        "currentPeriod": {"limit": 300.0, "spent": 100.0},
        "createdTime": "2024-01-01T10:00:00"
    }])

    valid_df, error_df = transform_cards(raw_df, "day_flat")

    assert valid_df.count() == 1
    assert error_df.count() == 0





# =========================================================
# 8. FLAT INPUT – DEDUPLICATION
# =========================================================
def test_transform_cards_flat_input_deduplication(spark):
    raw_df = create_flat_cards_df(spark, [
        {
            "id": "cf2",
            "status": "active",
            "type": "virtual",
            "currentPeriod": {"limit": 100.0, "spent": 10.0},
            "createdTime": "2024-01-01T10:00:00"
        },
        {
            "id": "cf2",
            "status": "active",
            "type": "virtual",
            "currentPeriod": {"limit": 100.0, "spent": 10.0},
            "createdTime": "2024-01-01T10:00:00"
        }
    ])

    valid_df, error_df = transform_cards(raw_df, "day_flat")

    assert valid_df.count() == 1
    assert error_df.count() == 0
