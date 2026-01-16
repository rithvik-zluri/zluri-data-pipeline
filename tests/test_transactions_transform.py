# tests/pipelines/transactions/test_transactions_transform.py

import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType, ArrayType, TimestampType
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
        .appName("transactions-transform-test")
        .getOrCreate()
    )


# -------------------------------------------------------------------
# RAW SCHEMA (matches input structure)
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


# -------------------------------------------------------------------
# 1. API RATE PATH
# -------------------------------------------------------------------
@patch("src.pipelines.transactions.transactions_transform.get_rate_to_usd")
def test_api_rate_used(mock_get_rate, spark):
    mock_get_rate.return_value = 0.5

    data = [{
        "results": [{
            "id": "t1",
            "uuid": "u1",
            "occurredTime": "2024-01-01T10:00:00",
            "updatedTime": "2024-01-01T11:00:00",
            "userId": "user1",
            "userUuid": "uu1",
            "userName": "Test User",
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
    }]

    df = spark.createDataFrame(data, schema=raw_schema())
    valid_df, error_df, _ = transform_transactions(df, "day1")

    row = valid_df.collect()[0]

    assert row["original_amount"] == 10.0
    assert row["exchange_rate"] == 0.5
    assert row["amount_usd"] == 5.0
    assert error_df.count() == 0


# -------------------------------------------------------------------
# 2. PAYLOAD FALLBACK PATH
# -------------------------------------------------------------------
@patch("src.pipelines.transactions.transactions_transform.get_rate_to_usd")
def test_payload_fallback_used(mock_get_rate, spark):
    mock_get_rate.return_value = None

    data = [{
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
    }]

    df = spark.createDataFrame(data, schema=raw_schema())
    valid_df, error_df, _ = transform_transactions(df, "day1")

    row = valid_df.collect()[0]

    assert row["original_amount"] == 20.0
    assert row["exchange_rate"] == 2.0
    assert row["amount_usd"] == 40.0
    assert error_df.count() == 0


# -------------------------------------------------------------------
# 3. NO API + NO PAYLOAD (🔥 LINE 103 BRANCH)
# -------------------------------------------------------------------
@patch("src.pipelines.transactions.transactions_transform.get_rate_to_usd")
def test_no_api_and_no_payload_rate(mock_get_rate, spark):
    mock_get_rate.return_value = None

    data = [{
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
    }]

    df = spark.createDataFrame(data, schema=raw_schema())
    valid_df, error_df, _ = transform_transactions(df, "day1")

    assert valid_df.count() == 0
    assert error_df.count() == 1

    err = error_df.collect()[0]
    assert "exchange_rate not found" in err["error_message"]


# -------------------------------------------------------------------
# 4. DEDUPLICATION TEST
# -------------------------------------------------------------------
@patch("src.pipelines.transactions.transactions_transform.get_rate_to_usd")
def test_deduplication(mock_get_rate, spark):
    mock_get_rate.return_value = 1.0

    data = [{
        "results": [
            {
                "id": "t4",
                "uuid": "u4",
                "occurredTime": "2024-01-04T10:00:00",
                "updatedTime": "2024-01-04T11:00:00",
                "userId": "user4",
                "userUuid": "uu4",
                "userName": "Dup User",
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
                "userName": "Dup User",
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
    }]

    df = spark.createDataFrame(data, schema=raw_schema())
    valid_df, error_df, _ = transform_transactions(df, "day1")

    assert valid_df.count() == 1
    assert error_df.count() == 0


# -------------------------------------------------------------------
# 5. VALIDATION ERROR TEST
# -------------------------------------------------------------------
@patch("src.pipelines.transactions.transactions_transform.get_rate_to_usd")
def test_validation_error_transaction_id_null(mock_get_rate, spark):
    mock_get_rate.return_value = 1.0

    data = [{
        "results": [{
            "id": None,
            "uuid": "u5",
            "occurredTime": "2024-01-05T10:00:00",
            "updatedTime": "2024-01-05T11:00:00",
            "userId": "user5",
            "userUuid": "uu5",
            "userName": "Invalid User",
            "merchantName": "Swiggy",
            "rawMerchantName": "SWG",
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
    }]

    df = spark.createDataFrame(data, schema=raw_schema())
    valid_df, error_df, _ = transform_transactions(df, "day1")

    assert valid_df.count() == 0
    assert error_df.count() == 1

    err = error_df.collect()[0]
    assert err["error_message"] == "transaction_id is null"
