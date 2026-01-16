import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, StructType, ArrayType
from src.pipelines.cards.cards_transform import transform_cards


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("cards-transform-tests") \
        .getOrCreate()


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
# 1. HAPPY PATH
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

    row = valid_df.collect()[0]
    assert row.card_id == "c1"
    assert row.status == "active"
    assert row.current_limit == 500.0
    assert row.current_spent == 200.0
    assert row.sync_day == "day1"


# =========================================================
# 2. NULL MANDATORY FIELD → ERROR
# =========================================================
def test_transform_cards_null_card_id(spark):
    raw_df = create_raw_cards_df(spark, [{
        "id": None,
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

    assert valid_df.count() == 0
    assert error_df.count() == 1


# =========================================================
# 3. NEGATIVE NUMBERS → ERROR
# =========================================================
def test_transform_cards_negative_values(spark):
    raw_df = create_raw_cards_df(spark, [{
        "id": "c2",
        "uuid": "u2",
        "name": "Card 2",
        "userId": "user2",
        "userUuid": "uu2",
        "budgetId": "b2",
        "budgetUuid": "bu2",
        "lastFour": "9999",
        "validThru": "01/26",
        "status": "active",
        "type": "physical",
        "shareBudgetFunds": "true",
        "recurring": "true",
        "recurringLimit": -10.0,  # invalid
        "currentPeriod": {"limit": -100.0, "spent": -20.0},  # invalid
        "createdTime": "2024-01-01T10:00:00",
        "updatedTime": "2024-01-02T10:00:00"
    }])

    valid_df, error_df = transform_cards(raw_df, "day1")

    assert valid_df.count() == 0
    assert error_df.count() == 1


# =========================================================
# 4. NULL OPTIONAL NUMBERS → STILL VALID
# =========================================================
def test_transform_cards_null_optional_numbers(spark):
    raw_df = create_raw_cards_df(spark, [{
        "id": "c3",
        "uuid": "u3",
        "name": "Card 3",
        "userId": "user3",
        "userUuid": "uu3",
        "budgetId": "b3",
        "budgetUuid": "bu3",
        "lastFour": "1111",
        "validThru": "01/26",
        "status": "inactive",
        "type": "virtual",
        "shareBudgetFunds": "false",
        "recurring": "false",
        "recurringLimit": None,
        "currentPeriod": {"limit": None, "spent": None},
        "createdTime": "2024-01-01T10:00:00",
        "updatedTime": "2024-01-02T10:00:00"
    }])

    valid_df, error_df = transform_cards(raw_df, "day1")

    assert valid_df.count() == 1
    assert error_df.count() == 0


# =========================================================
# 5. DEDUPLICATION
# =========================================================
def test_transform_cards_deduplication(spark):
    raw_df = create_raw_cards_df(spark, [
        {
            "id": "c4",
            "uuid": "u4",
            "name": "Card 4",
            "userId": "user4",
            "userUuid": "uu4",
            "budgetId": "b4",
            "budgetUuid": "bu4",
            "lastFour": "4444",
            "validThru": "01/26",
            "status": "active",
            "type": "virtual",
            "shareBudgetFunds": "true",
            "recurring": "false",
            "recurringLimit": 10.0,
            "currentPeriod": {"limit": 100.0, "spent": 10.0},
            "createdTime": "2024-01-01T10:00:00",
            "updatedTime": "2024-01-02T10:00:00"
        },
        {
            "id": "c4",  # duplicate
            "uuid": "u4",
            "name": "Card 4 Duplicate",
            "userId": "user4",
            "userUuid": "uu4",
            "budgetId": "b4",
            "budgetUuid": "bu4",
            "lastFour": "4444",
            "validThru": "01/26",
            "status": "active",
            "type": "virtual",
            "shareBudgetFunds": "true",
            "recurring": "false",
            "recurringLimit": 10.0,
            "currentPeriod": {"limit": 100.0, "spent": 10.0},
            "createdTime": "2024-01-01T10:00:00",
            "updatedTime": "2024-01-02T10:00:00"
        }
    ])

    valid_df, error_df = transform_cards(raw_df, "day1")

    assert valid_df.count() == 1
    assert error_df.count() == 0
