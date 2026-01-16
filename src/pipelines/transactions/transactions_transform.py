# src/pipelines/transactions/transactions_transform.py

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructType,
    StructField
)
from src.utils.currency_converter import get_rate_to_usd


def transform_transactions(raw_df, day: str):
    print(f"=== Starting Transactions Transform for {day} ===")

    # ---------------------------------------------------------
    # EXPLODE RESULTS
    # ---------------------------------------------------------
    df = raw_df.select(F.explode("results").alias("txn"))

    # ---------------------------------------------------------
    # SELECT & RENAME
    # ---------------------------------------------------------
    df = df.select(
        F.col("txn.id").alias("transaction_id"),
        F.col("txn.uuid").alias("transaction_uuid"),

        F.to_timestamp("txn.occurredTime").alias("occurred_time"),
        F.to_timestamp("txn.updatedTime").alias("updated_time"),

        F.col("txn.userId").alias("user_id"),
        F.col("txn.userUuid").alias("user_uuid"),
        F.col("txn.userName").alias("user_name"),

        F.col("txn.merchantName").alias("merchant_name"),
        F.col("txn.rawMerchantName").alias("raw_merchant_name"),

        F.col("txn.cardId").alias("card_id"),
        F.col("txn.cardUuid").alias("card_uuid"),

        F.col("txn.budgetId").alias("budget_id"),
        F.col("txn.budgetUuid").alias("budget_uuid"),

        (
            F.col("txn.currencyData.originalCurrencyAmount").cast(DoubleType())
            / (10 ** F.col("txn.currencyData.exponent"))
        ).alias("original_amount"),

        F.col("txn.currencyData.originalCurrencyCode").alias("original_currency"),

        # Payload fallback rate
        F.col("txn.currencyData.exchangeRate").cast(DoubleType()).alias("payload_exchange_rate"),

        # Raw JSON
        F.to_json(F.col("txn")).alias("raw_payload")
    )

    # ---------------------------------------------------------
    # IDEMPOTENCY KEY
    # ---------------------------------------------------------
    df = df.withColumn(
        "idempotency_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("transaction_id"),
                F.col("transaction_uuid"),
                F.col("occurred_time").cast(StringType()),
                F.col("original_amount").cast(StringType()),
                F.col("original_currency")
            ),
            256
        )
    )

    # ---------------------------------------------------------
    # DEDUPLICATION
    # ---------------------------------------------------------
    df = df.dropDuplicates(["idempotency_key"])

    # ---------------------------------------------------------
    # FX DATE
    # ---------------------------------------------------------
    df = df.withColumn("fx_date", F.to_date("occurred_time"))

    # ---------------------------------------------------------
    # PRE-COMPUTE FX MAP (API)
    # ---------------------------------------------------------
    fx_pairs = df.select("fx_date", "original_currency").distinct().collect()

    fx_map = {}
    for r in fx_pairs:
        if r["fx_date"] and r["original_currency"]:
            fx_map[(r["fx_date"], r["original_currency"])] = get_rate_to_usd(
                r["fx_date"],
                r["original_currency"]
            )

    bc_fx = df.sparkSession.sparkContext.broadcast(fx_map)

    # ---------------------------------------------------------
    # FX UDF (API FIRST → PAYLOAD FALLBACK)
    # ---------------------------------------------------------
    def lookup_rate_and_source(date_val, currency_val, payload_rate):
        api_rate = bc_fx.value.get((date_val, currency_val))

        if api_rate is not None:
            return api_rate, "API"

        if payload_rate is not None:
            return float(payload_rate), "PAYLOAD"

        return None, "MISSING"

    fx_schema = StructType([
        StructField("exchange_rate", DoubleType(), True),
        StructField("fx_source", StringType(), True)
    ])

    fx_udf = F.udf(lookup_rate_and_source, fx_schema)

    df = df.withColumn(
        "fx_struct",
        fx_udf("fx_date", "original_currency", "payload_exchange_rate")
    )

    df = (
        df.withColumn("exchange_rate", F.col("fx_struct.exchange_rate"))
          .withColumn("fx_source", F.col("fx_struct.fx_source"))
          .drop("fx_struct")
    )

    # ---------------------------------------------------------
    # USD AMOUNT
    # ---------------------------------------------------------
    df = df.withColumn(
        "amount_usd",
        F.col("original_amount") * F.col("exchange_rate")
    )

    # ---------------------------------------------------------
    # VALIDATIONS
    # ---------------------------------------------------------
    df = df.withColumn(
        "error_message",
        F.when(F.col("transaction_id").isNull(), "transaction_id is null")
         .when(F.col("transaction_uuid").isNull(), "transaction_uuid is null")
         .when(F.col("occurred_time").isNull(), "occurred_time is null")
         .when(F.col("original_currency").isNull(), "original_currency is null")
         .when(F.col("original_amount").isNull(), "original_amount is null")
         .when(F.col("exchange_rate").isNull(), "exchange_rate not found (api + payload)")
         .otherwise(None)
    )

    # ---------------------------------------------------------
    # SPLIT VALID / ERROR
    # ---------------------------------------------------------
    valid_df = df.filter(F.col("error_message").isNull())
    error_df = df.filter(F.col("error_message").isNotNull())

    # ---------------------------------------------------------
    # SHAPE VALID DF
    # ---------------------------------------------------------
    valid_df = valid_df.select(
        "transaction_id",
        "transaction_uuid",
        "occurred_time",
        "updated_time",
        "user_id",
        "user_uuid",
        "user_name",
        "merchant_name",
        "raw_merchant_name",
        "card_id",
        "card_uuid",
        "budget_id",
        "budget_uuid",
        "original_amount",
        "original_currency",
        "amount_usd",
        "exchange_rate",
        "fx_source",
        "idempotency_key",
        "raw_payload"
    )

    # ---------------------------------------------------------
    # SHAPE ERROR DF
    # ---------------------------------------------------------
    error_df = error_df.select(
        F.col("transaction_id"),
        F.lit("VALIDATION_ERROR").alias("error_type"),
        F.col("error_message"),
        F.col("raw_payload").alias("raw_record")
    )

    # ---------------------------------------------------------
    # PIPELINE STATE (IMPORTANT)
    # ---------------------------------------------------------
    pipeline_state_df = df.select(
        F.lit("transactions").alias("pipeline_name"),
        F.max("updated_time").alias("last_processed_time")
    )

    # ---------------------------------------------------------
    # LOGS
    # ---------------------------------------------------------
    print(f"📊 Total records  : {df.count()}")
    print(f"✅ Valid records  : {valid_df.count()}")
    print(f"❌ Error records  : {error_df.count()}")

    return valid_df, error_df, pipeline_state_df
