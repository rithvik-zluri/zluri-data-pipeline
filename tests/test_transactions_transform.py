import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    IntegerType, DoubleType, ArrayType
)

from src.pipelines.transactions.transactions_transform import transform_transactions


# -------------------------------------------------------------------
# SPARK FIXTURE
# -------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("transactions-transform-tests")
        .getOrCreate()
    )


# -------------------------------------------------------------------
# RAW INPUT SCHEMA (matches ingestion output)
# -------------------------------------------------------------------
def raw_schema():
    return StructType([
        StructField("results", ArrayType(
            StructType([
                StructField("id", StringType(), True),
                StructField("uuid", StringType(), True),
                StructField("occurredTime", StringType(), True),
                StructField("updatedTime", StringType(), True),
                StructField("userId", StringType(), True),
                StructField("userUuid", StringType(), True),
                StructField("userName", StringType(), True),
                StructField("merchantName", StringType(), True),
                StructField("rawMerchantName", StringType(), True),
                StructField("cardId", StringType(), True),
                StructField("cardUuid", StringType(), True),
                StructField("budgetId", StringType(), True),
                StructField("budgetUuid", StringType(), True),
                StructField("currencyData", StructType([
                    StructField("originalCurrencyAmount", LongType(), True),
                    StructField("exponent", IntegerType(), True),
                    StructField("originalCurrencyCode", StringType(), True),
                    StructField("exchangeRate", DoubleType(), True),
                ]), True),
            ])
        ), True)
    ])


# ============================================================
# 1. API FX RATE PATH
# ============================================================
@patch("src.pipelines.transactions.transactions_transform.get_rate_to_usd")
def test_api_rate_used(mock_get_rate, spark):
    mock_get_rate.return_value = 0.5

    df = spark.createDataFrame([{
        "results": [{
            "id": "t1",
            "uuid": "u1",
            "occurredTime": "2024-01-01T10:00:00",
            "updatedTime": "2024-01-01T11:00:00",
            "userId": "user1",
            "userUuid": "uu1",
            "userName": "API User",
            "merchantName": "Amazon",
            "rawMerchantName": "AMZN",
            "cardId": "c1",
            "cardUuid": "cu1",
            "budgetId": "b1",
            "budgetUuid": "bu1",
            "currencyData": {
                "originalCurrencyAmount": 1000,
                "exponent": 2,
                "originalCurrencyCode": "INR",
                "exchangeRate": None
            }
        }]
    }], schema=raw_schema())

    valid_df, error_df, state_df = transform_transactions(df, "day1")

    row = valid_df.orderBy("transaction_id").first()

    assert row["original_amount"] == 10.0
    assert row["exchange_rate"] == 0.5
    assert row["amount_usd"] == 5.0
    assert row["fx_source"] == "API"
    assert error_df.count() == 0

    state = state_df.collect()[0]
    assert state["pipeline_name"] == "transactions"
    assert state["last_processed_time"] is not None


# ============================================================
# 2. PAYLOAD FALLBACK PATH
# ============================================================
@patch("src.pipelines.transactions.transactions_transform.get_rate_to_usd")
def test_payload_fallback_used(mock_get_rate, spark):
    mock_get_rate.return_value = None

    df = spark.createDataFrame([{
        "results": [{
            "id": "t2",
            "uuid": "u2",
            "occurredTime": "2024-01-02T10:00:00",
            "updatedTime": "2024-01-02T11:00:00",
            "userId": "user2",
            "userUuid": "uu2",
            "userName": "Payload User",
            "merchantName": "Flipkart",
            "rawMerchantName": "FLPK",
            "cardId": "c2",
            "cardUuid": "cu2",
            "budgetId": "b2",
            "budgetUuid": "bu2",
            "currencyData": {
                "originalCurrencyAmount": 2000,
                "exponent": 2,
                "originalCurrencyCode": "EUR",
                "exchangeRate": 2.0
            }
        }]
    }], schema=raw_schema())

    valid_df, error_df, _ = transform_transactions(df, "day1")
    row = valid_df.first()

    assert row["exchange_rate"] == 2.0
    assert row["fx_source"] == "PAYLOAD"
    assert row["amount_usd"] == 40.0
    assert error_df.count() == 0


# ============================================================
# 3. NO API + NO PAYLOAD → ERROR
# ============================================================
@patch("src.pipelines.transactions.transactions_transform.get_rate_to_usd")
def test_no_fx_rate_available(mock_get_rate, spark):
    mock_get_rate.return_value = None

    df = spark.createDataFrame([{
        "results": [{
            "id": "t3",
            "uuid": "u3",
            "occurredTime": "2024-01-03T10:00:00",
            "updatedTime": "2024-01-03T11:00:00",
            "userId": "user3",
            "userUuid": "uu3",
            "userName": "NoRate",
            "merchantName": "Unknown",
            "rawMerchantName": "UNK",
            "cardId": "c3",
            "cardUuid": "cu3",
            "budgetId": "b3",
            "budgetUuid": "bu3",
            "currencyData": {
                "originalCurrencyAmount": 3000,
                "exponent": 2,
                "originalCurrencyCode": "JPY",
                "exchangeRate": None
            }
        }]
    }], schema=raw_schema())

    valid_df, error_df, _ = transform_transactions(df, "day1")

    assert valid_df.count() == 0
    assert "exchange_rate not found" in error_df.first()["error_message"]


# ============================================================
# 4. DEDUPLICATION
# ============================================================
@patch("src.pipelines.transactions.transactions_transform.get_rate_to_usd")
def test_deduplication(mock_get_rate, spark):
    mock_get_rate.return_value = 1.0

    df = spark.createDataFrame([{
        "results": [
            {
                "id": "t4",
                "uuid": "u4",
                "occurredTime": "2024-01-04T10:00:00",
                "updatedTime": "2024-01-04T11:00:00",
                "userId": "user4",
                "userUuid": "uu4",
                "userName": "Dup",
                "merchantName": "Zomato",
                "rawMerchantName": "ZMT",
                "cardId": "c4",
                "cardUuid": "cu4",
                "budgetId": "b4",
                "budgetUuid": "bu4",
                "currencyData": {
                    "originalCurrencyAmount": 4000,
                    "exponent": 2,
                    "originalCurrencyCode": "USD",
                    "exchangeRate": 1.0
                }
            },
            {
                "id": "t4",
                "uuid": "u4",
                "occurredTime": "2024-01-04T10:00:00",
                "updatedTime": "2024-01-04T11:00:00",
                "userId": "user4",
                "userUuid": "uu4",
                "userName": "Dup",
                "merchantName": "Zomato",
                "rawMerchantName": "ZMT",
                "cardId": "c4",
                "cardUuid": "cu4",
                "budgetId": "b4",
                "budgetUuid": "bu4",
                "currencyData": {
                    "originalCurrencyAmount": 4000,
                    "exponent": 2,
                    "originalCurrencyCode": "USD",
                    "exchangeRate": 1.0
                }
            }
        ]
    }], schema=raw_schema())

    valid_df, error_df, _ = transform_transactions(df, "day1")

    assert valid_df.count() == 1
    assert error_df.count() == 0


# ============================================================
# 5. VALIDATION: occurred_time NULL
# ============================================================
@patch("src.pipelines.transactions.transactions_transform.get_rate_to_usd")
def test_occurred_time_null(mock_get_rate, spark):
    mock_get_rate.return_value = 1.0

    df = spark.createDataFrame([{
        "results": [{
            "id": "t5",
            "uuid": "u5",
            "occurredTime": None,
            "updatedTime": "2024-01-05T11:00:00",
            "userId": "user5",
            "userUuid": "uu5",
            "userName": "BadTime",
            "merchantName": "Test",
            "rawMerchantName": "TST",
            "cardId": "c5",
            "cardUuid": "cu5",
            "budgetId": "b5",
            "budgetUuid": "bu5",
            "currencyData": {
                "originalCurrencyAmount": 5000,
                "exponent": 2,
                "originalCurrencyCode": "USD",
                "exchangeRate": 1.0
            }
        }]
    }], schema=raw_schema())

    valid_df, error_df, _ = transform_transactions(df, "day1")

    assert valid_df.count() == 0
    assert error_df.first()["error_message"] == "occurred_time is null"


# ============================================================
# 6. IDEMPOTENCY KEY STABILITY
# ============================================================
@patch("src.pipelines.transactions.transactions_transform.get_rate_to_usd")
def test_idempotency_key_deterministic(mock_get_rate, spark):
    mock_get_rate.return_value = 1.0

    df = spark.createDataFrame([{
        "results": [{
            "id": "t6",
            "uuid": "u6",
            "occurredTime": "2024-01-06T10:00:00",
            "updatedTime": "2024-01-06T11:00:00",
            "userId": "user6",
            "userUuid": "uu6",
            "userName": "Stable",
            "merchantName": "Test",
            "rawMerchantName": "TST",
            "cardId": "c6",
            "cardUuid": "cu6",
            "budgetId": "b6",
            "budgetUuid": "bu6",
            "currencyData": {
                "originalCurrencyAmount": 6000,
                "exponent": 2,
                "originalCurrencyCode": "USD",
                "exchangeRate": 1.0
            }
        }]
    }], schema=raw_schema())

    valid_df, _, _ = transform_transactions(df, "day1")

    key1 = valid_df.first()["idempotency_key"]
    key2 = valid_df.first()["idempotency_key"]

    assert key1 == key2
