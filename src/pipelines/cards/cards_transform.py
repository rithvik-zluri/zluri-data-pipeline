from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    lit,
    current_timestamp,
    to_timestamp,
    struct,
    to_json,
    get_json_object
)


def transform_cards(raw_df: DataFrame, day: str):

    # --------------------------------------------------
    # NORMALIZE INPUT (nested OR flat)
    # --------------------------------------------------
    if "results" in raw_df.columns:
        base_df = raw_df.select(explode(col("results")).alias("card"))
    else:
        base_df = raw_df.select(struct(*raw_df.columns).alias("card"))

    card_json = to_json(col("card"))

    # --------------------------------------------------
    # SAFE FIELD EXTRACTION (NO FIELD_NOT_FOUND)
    # --------------------------------------------------
    cards_df = base_df.select(
        get_json_object(card_json, "$.id").alias("card_id"),
        get_json_object(card_json, "$.uuid").alias("card_uuid"),
        get_json_object(card_json, "$.name").alias("name"),

        get_json_object(card_json, "$.userId").alias("user_id"),
        get_json_object(card_json, "$.userUuid").alias("user_uuid"),

        get_json_object(card_json, "$.budgetId").alias("budget_id"),
        get_json_object(card_json, "$.budgetUuid").alias("budget_uuid"),

        get_json_object(card_json, "$.lastFour").alias("last_four"),
        get_json_object(card_json, "$.validThru").alias("valid_thru"),

        get_json_object(card_json, "$.status").alias("status"),
        get_json_object(card_json, "$.type").alias("type"),

        get_json_object(card_json, "$.shareBudgetFunds").cast("boolean").alias("share_budget_funds"),
        get_json_object(card_json, "$.recurring").cast("boolean").alias("recurring"),
        get_json_object(card_json, "$.recurringLimit").cast("double").alias("recurring_limit"),

        get_json_object(card_json, "$.currentPeriod.limit").cast("double").alias("current_limit"),
        get_json_object(card_json, "$.currentPeriod.spent").cast("double").alias("current_spent"),

        to_timestamp(get_json_object(card_json, "$.createdTime")).alias("created_time"),
        to_timestamp(get_json_object(card_json, "$.updatedTime")).alias("updated_time"),

        lit(day).alias("sync_day"),
        lit("api").alias("source"),
        current_timestamp().alias("ingested_at")
    )

    # --------------------------------------------------
    # VALIDATION
    # --------------------------------------------------
    error_df = cards_df.filter(
        col("card_id").isNull()
        | col("status").isNull()
        | col("type").isNull()
        | col("created_time").isNull()
        | (col("current_limit") < 0)
        | (col("current_spent") < 0)
        | (col("recurring_limit") < 0)
    ).select(
        col("card_id"),
        lit("VALIDATION_ERROR").alias("error_type"),
        lit("One or more mandatory fields are invalid").alias("error_message"),
        to_json(struct(*cards_df.columns)).alias("raw_record"),
        col("sync_day"),
        current_timestamp().alias("created_at")
    )

    valid_df = cards_df.filter(
        col("card_id").isNotNull()
        & col("status").isNotNull()
        & col("type").isNotNull()
        & col("created_time").isNotNull()
        & (col("current_limit").isNull() | (col("current_limit") >= 0))
        & (col("current_spent").isNull() | (col("current_spent") >= 0))
        & (col("recurring_limit").isNull() | (col("recurring_limit") >= 0))
    )

    # --------------------------------------------------
    # DEDUPLICATION
    # --------------------------------------------------
    valid_df = valid_df.dropDuplicates(["card_id"])

    return valid_df, error_df
