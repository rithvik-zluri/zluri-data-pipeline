from pyspark.sql.functions import (
    col, explode, lit, current_timestamp, to_timestamp, struct, to_json
)

def transform_cards(raw_df, day: str):
    exploded_df = raw_df.select(explode(col("results")).alias("card"))

    cards_df = exploded_df.select(
        col("card.id").alias("card_id"),
        col("card.uuid").alias("card_uuid"),
        col("card.name").alias("name"),

        col("card.userId").alias("user_id"),
        col("card.userUuid").alias("user_uuid"),

        col("card.budgetId").alias("budget_id"),
        col("card.budgetUuid").alias("budget_uuid"),

        col("card.lastFour").alias("last_four"),
        col("card.validThru").alias("valid_thru"),

        col("card.status").alias("status"),
        col("card.type").alias("type"),

        col("card.shareBudgetFunds").alias("share_budget_funds"),
        col("card.recurring").alias("recurring"),
        col("card.recurringLimit").alias("recurring_limit"),

        col("card.currentPeriod.limit").alias("current_limit"),
        col("card.currentPeriod.spent").alias("current_spent"),

        to_timestamp(col("card.createdTime")).alias("created_time"),
        to_timestamp(col("card.updatedTime")).alias("updated_time"),

        lit(day).alias("sync_day"),
        lit("api").alias("source"),
        current_timestamp().alias("ingested_at")
    )

    # ---------------------------
    # VALIDATION (FIXED)
    # ---------------------------
    error_df = cards_df.filter(
        col("card_id").isNull() |
        col("status").isNull() |
        col("type").isNull() |
        col("created_time").isNull() |
        (col("current_limit") < 0) |
        (col("current_spent") < 0) |
        (col("recurring_limit") < 0)
    ).select(
        col("card_id"),
        lit("VALIDATION_ERROR").alias("error_type"),
        lit("One or more mandatory fields are invalid").alias("error_message"),
        to_json(struct(*cards_df.columns)).alias("raw_record"),
        col("sync_day"),
        current_timestamp().alias("created_at")
    )

    valid_df = cards_df.filter(
        col("card_id").isNotNull() &
        col("status").isNotNull() &
        col("type").isNotNull() &
        col("created_time").isNotNull() &
        (col("current_limit").isNull() | (col("current_limit") >= 0)) &
        (col("current_spent").isNull() | (col("current_spent") >= 0)) &
        (col("recurring_limit").isNull() | (col("recurring_limit") >= 0))
    )

    # ---------------------------
    # DEDUPLICATION (within batch)
    # ---------------------------
    valid_df = valid_df.dropDuplicates(["card_id"])

    return valid_df, error_df
