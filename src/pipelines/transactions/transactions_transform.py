from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
from src.utils.currency_converter import get_rate_to_usd


def transform_transactions(raw_df, day: str):
    print(f"=== Starting Transactions Transform for {day} ===")

    # ---------------------------------------------------------
    # NORMALIZE INPUT (NESTED OR FLAT)
    # ---------------------------------------------------------
    if "results" in raw_df.columns:
        base_df = raw_df.select(F.explode("results").alias("txn"))
    else:
        base_df = raw_df.select(F.struct(*raw_df.columns).alias("txn"))

    txn_json = F.to_json(F.col("txn"))

    # ---------------------------------------------------------
    # SAFE FIELD EXTRACTION (NO SCHEMA ASSUMPTIONS)
    # ---------------------------------------------------------
    amount_raw = F.coalesce(
        F.get_json_object(txn_json, "$.currencyData.originalCurrencyAmount"),
        F.get_json_object(txn_json, "$.originalCurrencyAmount")
    ).cast(DoubleType())

    exponent = F.coalesce(
        F.get_json_object(txn_json, "$.currencyData.exponent"),
        F.get_json_object(txn_json, "$.exponent")
    ).cast("int")

    original_currency = F.coalesce(
        F.get_json_object(txn_json, "$.currencyData.originalCurrencyCode"),
        F.get_json_object(txn_json, "$.originalCurrencyCode")
    )

    payload_exchange_rate = F.coalesce(
        F.get_json_object(txn_json, "$.currencyData.exchangeRate"),
        F.get_json_object(txn_json, "$.exchangeRate")
    ).cast(DoubleType())

    df = base_df.select(
        F.get_json_object(txn_json, "$.id").alias("transaction_id"),
        F.get_json_object(txn_json, "$.uuid").alias("transaction_uuid"),

        F.to_timestamp(F.get_json_object(txn_json, "$.occurredTime")).alias("occurred_time"),
        F.to_timestamp(F.get_json_object(txn_json, "$.updatedTime")).alias("updated_time"),

        F.get_json_object(txn_json, "$.userId").alias("user_id"),
        F.get_json_object(txn_json, "$.userUuid").alias("user_uuid"),
        F.get_json_object(txn_json, "$.userName").alias("user_name"),

        F.get_json_object(txn_json, "$.merchantName").alias("merchant_name"),
        F.get_json_object(txn_json, "$.rawMerchantName").alias("raw_merchant_name"),

        F.get_json_object(txn_json, "$.cardId").alias("card_id"),
        F.get_json_object(txn_json, "$.cardUuid").alias("card_uuid"),

        F.get_json_object(txn_json, "$.budgetId").alias("budget_id"),
        F.get_json_object(txn_json, "$.budgetUuid").alias("budget_uuid"),

        (amount_raw / F.pow(F.lit(10), exponent)).alias("original_amount"),
        original_currency.alias("original_currency"),
        payload_exchange_rate.alias("payload_exchange_rate"),

        txn_json.alias("raw_payload")
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
                F.col("original_currency"),
            ),
            256,
        ),
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
    fx_pairs = (
        df.select("fx_date", "original_currency")
        .filter(F.col("fx_date").isNotNull() & F.col("original_currency").isNotNull())
        .distinct()
        .collect()
    )

    fx_map = {
        (r["fx_date"], r["original_currency"]): get_rate_to_usd(
            r["fx_date"], r["original_currency"]
        )
        for r in fx_pairs
    }

    bc_fx = df.sparkSession.sparkContext.broadcast(fx_map)

    # ---------------------------------------------------------
    # FX UDF (API → PAYLOAD → MISSING)
    # ---------------------------------------------------------
    def lookup_rate_and_source(date_val, currency_val, payload_rate):
        api_rate = bc_fx.value.get((date_val, currency_val))

        if api_rate is not None:
            return api_rate, "API"
        if payload_rate is not None:
            return float(payload_rate), "PAYLOAD"
        return None, "MISSING"

    fx_schema = StructType(
        [
            StructField("exchange_rate", DoubleType(), True),
            StructField("fx_source", StringType(), True),
        ]
    )

    fx_udf = F.udf(lookup_rate_and_source, fx_schema)

    df = df.withColumn(
        "fx_struct",
        fx_udf("fx_date", "original_currency", "payload_exchange_rate"),
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
        "amount_usd", F.col("original_amount") * F.col("exchange_rate")
    )

    # ---------------------------------------------------------
    # VALIDATIONS (0 or null amounts treated as invalid)
    # ---------------------------------------------------------
    df = df.withColumn(
        "error_message",
        F.when(F.col("transaction_id").isNull(), "transaction_id is null")
        .when(F.col("transaction_uuid").isNull(), "transaction_uuid is null")
        .when(F.col("occurred_time").isNull(), "occurred_time is null")
        .when(F.col("original_currency").isNull(), "original_currency is null")
        .when(F.col("original_amount").isNull() | (F.col("original_amount") == 0), "original_amount is null or zero")
        .when(F.col("exchange_rate").isNull(), "exchange_rate not found (api + payload)")
        .otherwise(None),
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
        "raw_payload",
    )

    # ---------------------------------------------------------
    # SHAPE ERROR DF
    # ---------------------------------------------------------
    error_df = error_df.select(
        F.col("transaction_id"),
        F.lit("VALIDATION_ERROR").alias("error_type"),
        F.col("error_message"),
        F.col("raw_payload").alias("raw_record"),
    )

    # ---------------------------------------------------------
    # PIPELINE STATE
    # ---------------------------------------------------------
    pipeline_state_df = df.select(
        F.lit("transactions").alias("pipeline_name"),
        F.max("updated_time").alias("last_processed_time"),
    )

    # ---------------------------------------------------------
    # LOGS
    # ---------------------------------------------------------
    print(f"Total records  : {df.count()}")
    print(f"Valid records  : {valid_df.count()}")
    print(f"Error records  : {error_df.count()}")

    return valid_df, error_df, pipeline_state_df
