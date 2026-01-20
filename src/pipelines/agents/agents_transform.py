from pyspark.sql import functions as F
from pyspark.sql.types import StructType


# -------------------------------------------------
# UTILS
# -------------------------------------------------
def ensure_column(df, col_name, default_value=None):
    if col_name not in df.columns:
        return df.withColumn(col_name, F.lit(default_value))
    return df


def safe_col(df, col_name):
    """
    Safely return a column if it exists, else NULL
    """
    if col_name in df.columns:
        return F.col(col_name)
    return F.lit(None)


def safe_nested_col(df, struct_col, nested_col):
    """
    Safely return struct_col.nested_col if it exists in schema.
    Works even if struct value is NULL at row level.
    """
    if struct_col in df.columns:
        field = df.schema[struct_col].dataType
        if isinstance(field, StructType) and nested_col in field.names:
            return F.col(f"{struct_col}.{nested_col}")
    return F.lit(None)


def safe_coalesce(df, nested_path, flat_col):
    """
    Coalesce nested + flat column safely
    """
    struct_col, nested_col = nested_path.split(".")
    return F.coalesce(
        safe_nested_col(df, struct_col, nested_col),
        safe_col(df, flat_col),
    )


# -------------------------------------------------
# TRANSFORM
# -------------------------------------------------
def transform_agents(agent_details_df, agents_df):
    """
    Supports:
    - Nested format: contact.email, contact.name
    - Flat format: email, name
    - Emits error_df for missing required fields
    """

    # -------------------------------------------------
    # 1. NORMALIZE REQUIRED FIELDS (FULLY SAFE)
    # -------------------------------------------------
    normalized_df = agent_details_df.select(
        # SAFE numeric cast (prevents job failure)
        F.expr("try_cast(id as bigint)").alias("agent_id"),

        safe_coalesce(agent_details_df, "contact.email", "email").alias("email"),
        safe_coalesce(agent_details_df, "contact.name", "name").alias("name"),
        safe_coalesce(agent_details_df, "contact.job_title", "job_title").alias("job_title"),
        safe_coalesce(agent_details_df, "contact.language", "language").alias("language"),
        safe_coalesce(agent_details_df, "contact.mobile", "mobile").alias("mobile"),
        safe_coalesce(agent_details_df, "contact.phone", "phone").alias("phone"),
        safe_coalesce(agent_details_df, "contact.time_zone", "time_zone").alias("time_zone"),

        F.to_timestamp(
            safe_coalesce(agent_details_df, "contact.last_login_at", "last_login_at")
        ).alias("last_login_at"),

        "*"
    )

    # -------------------------------------------------
    # 2. REQUIRED FIELD VALIDATION
    # -------------------------------------------------
    missing_required = (
        F.col("agent_id").isNull()
        | F.col("email").isNull()
        | F.col("name").isNull()
    )

    error_df = (
        normalized_df
        .filter(missing_required)
        .select(
            F.col("agent_id"),
            F.lit("MISSING_REQUIRED_FIELD").alias("error_type"),
            F.lit("agent_id, email, and name are required").alias("error_message"),
            F.to_json(F.struct("*")).alias("raw_record"),
        )
    )

    clean_df = normalized_df.filter(~missing_required)

    # -------------------------------------------------
    # 3. DEFENSIVE OPTIONAL COLUMNS
    # -------------------------------------------------
    optional_columns = [
        "available",
        "available_since",
        "deactivated",
        "focus_mode",
        "agent_operational_status",
        "last_active_at",
        "created_at",
        "updated_at",
        "ticket_scope",
        "org_agent_id",
        "signature",
        "freshchat_agent",
    ]

    for c in optional_columns:
        clean_df = ensure_column(clean_df, c)

    # -------------------------------------------------
    # 4. FINAL AGENTS SHAPE
    # -------------------------------------------------
    all_agents_df = clean_df.select(
        "agent_id",
        "email",
        "name",
        "job_title",
        "language",
        "mobile",
        "phone",
        "time_zone",
        "last_login_at",

        F.col("available"),
        F.col("available_since").cast("timestamp"),

        F.col("deactivated"),
        F.col("focus_mode"),
        F.col("agent_operational_status"),

        F.col("last_active_at").cast("timestamp"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp"),

        F.col("org_agent_id"),
        F.col("ticket_scope"),
        F.col("signature"),
        F.col("freshchat_agent"),
    )

    # -------------------------------------------------
    # 5. ACTIVE AGENTS
    # -------------------------------------------------
    active_agents_df = (
        agents_df
        .select(F.expr("try_cast(id as bigint)").alias("agent_id"))
        .distinct()
        .withColumn("is_active", F.lit(True))
    )

    # -------------------------------------------------
    # 6. DERIVE STATUS
    # -------------------------------------------------
    final_agents_df = (
        all_agents_df
        .join(active_agents_df, "agent_id", "left")
        .withColumn(
            "status",
            F.when(F.col("is_active") == True, F.lit("active"))
             .otherwise(F.lit("inactive"))
        )
        .drop("is_active")
    )

    return final_agents_df, error_df
