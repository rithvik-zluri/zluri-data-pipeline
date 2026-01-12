# src/pipelines/agents/agents_transform.py

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType


def ensure_column(df, col_name, default_value=None):
    if col_name not in df.columns:
        return df.withColumn(col_name, F.lit(default_value))
    return df


def transform_agents(agent_details_df, agents_df):
    # -------------------------------------------------
    # 1. ERROR DETECTION – missing critical fields
    # -------------------------------------------------
    error_condition = (
        F.col("id").isNull() |
        F.col("contact.email").isNull() |
        F.col("contact.name").isNull()
    )

    error_df = agent_details_df.filter(error_condition).select(
        F.col("id").alias("agent_id"),
        F.lit("MISSING_REQUIRED_FIELD").alias("error_type"),
        F.lit("One or more required fields are null").alias("error_message"),
        F.to_json(F.struct("*")).alias("raw_record")
    )

    # -------------------------------------------------
    # 2. CLEAN DATA ONLY
    # -------------------------------------------------
    clean_df = agent_details_df.filter(~error_condition)

    # -------------------------------------------------
    # 3. SAFELY ADD OPTIONAL COLUMNS IF MISSING
    # -------------------------------------------------
    optional_columns = [
        "available",
        "deactivated",
        "focus_mode",
        "agent_operational_status",
        "last_active_at",
        "created_at",
        "updated_at"
    ]

    for col_name in optional_columns:
        clean_df = ensure_column(clean_df, col_name, None)

    # -------------------------------------------------
    # 4. ALL AGENTS (normalized)
    # -------------------------------------------------
    all_agents_df = clean_df.select(
        F.col("id").cast("long").alias("agent_id"),
        F.col("contact.email").alias("email"),
        F.col("contact.name").alias("name"),
        F.col("contact.job_title").alias("job_title"),
        F.col("contact.language").alias("language"),
        F.col("contact.mobile").alias("mobile"),
        F.col("contact.phone").alias("phone"),
        F.col("contact.time_zone").alias("time_zone"),
        F.col("available"),
        F.col("deactivated"),
        F.col("focus_mode"),
        F.col("agent_operational_status"),
        F.col("last_active_at").cast("timestamp"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp")
    )

    # -------------------------------------------------
    # 5. HANDLE EMPTY agents_df SAFELY
    # -------------------------------------------------
    if agents_df.rdd.isEmpty():
        spark = agent_details_df.sparkSession
        empty_schema = StructType([
            StructField("agent_id", LongType(), True)
        ])
        active_agents_df = spark.createDataFrame([], empty_schema).withColumn("is_active", F.lit(True))
    else:
        active_agents_df = agents_df.select(
            F.col("id").cast("long").alias("agent_id")
        ).withColumn("is_active", F.lit(True))

    # -------------------------------------------------
    # 6. JOIN → DERIVE STATUS
    # -------------------------------------------------
    final_agents_df = all_agents_df.join(
        active_agents_df,
        on="agent_id",
        how="left"
    ).withColumn(
        "status",
        F.when(F.col("is_active").isNotNull(), F.lit("active"))
         .otherwise(F.lit("inactive"))
    ).drop("is_active")

    return final_agents_df, error_df
