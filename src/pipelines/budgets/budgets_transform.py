from pyspark.sql.functions import col, explode, to_date, lit, current_timestamp, struct, to_json


def transform_budgets(raw_df, day: str):
    exploded_df = raw_df.select(explode(col("results")).alias("budget"))

    budgets_df = exploded_df.select(
        col("budget.id").alias("budget_id"),
        col("budget.uuid").alias("budget_uuid"),
        col("budget.name").alias("name"),
        col("budget.description").alias("description"),
        col("budget.retired").alias("retired"),
        to_date(col("budget.startDate"), "yyyy-MM-dd").alias("start_date"),
        col("budget.recurringInterval").alias("recurring_interval"),
        col("budget.timezone").alias("timezone"),
        col("budget.currentPeriod.limit").alias("limit_amount"),
        col("budget.currentPeriod.overspendBuffer").alias("overspend_buffer"),
        col("budget.currentPeriod.assigned").alias("assigned_amount"),
        col("budget.currentPeriod.spent.cleared").alias("spent_cleared"),
        col("budget.currentPeriod.spent.pending").alias("spent_pending"),
        col("budget.currentPeriod.spent.total").alias("spent_total"),
        lit(day).alias("sync_day"),
        lit("api").alias("source"),
        current_timestamp().alias("ingested_at")
    )

    # ---------------------------
    # VALIDATION
    # ---------------------------
    error_df = budgets_df.filter(
        col("budget_id").isNull() | col("limit_amount").isNull()
    ).select(
        col("budget_id"),
        lit("VALIDATION_ERROR").alias("error_type"),
        lit("budget_id or limit_amount is null").alias("error_message"),
        to_json(struct(*budgets_df.columns)).alias("raw_record"),
        col("sync_day"),
        current_timestamp().alias("created_at")
    )

    valid_df = budgets_df.filter(
        col("budget_id").isNotNull() & col("limit_amount").isNotNull()
    )

    # ---------------------------
    # DEDUPLICATION (within batch)
    # ---------------------------
    valid_df = valid_df.dropDuplicates(["budget_id"])

    return valid_df, error_df
