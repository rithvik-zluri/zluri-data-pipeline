from pyspark.sql import functions as F

def ensure_column(df, col_name, default_value=None):
    if col_name not in df.columns:
        return df.withColumn(col_name, F.lit(default_value))
    return df


def transform_agents(agent_details_df, agents_df):
    """
    agent_details_df:
    - sample_data/sync-dayX/agent_details/*.json
    - ONE AGENT PER FILE
    - ONE ROW PER AGENT

    agents_df:
    - sample_data/sync-dayX/agents/1.json
    - ONE AGENT PER ROW
    """

    # -------------------------------------------------
    # 1. ERROR DETECTION
    # -------------------------------------------------
    error_condition = (
        F.col("id").isNull()
        | F.col("contact.email").isNull()
        | F.col("contact.name").isNull()
    )

    error_df = (
        agent_details_df
        .filter(error_condition)
        .select(
            F.col("id").cast("long").alias("agent_id"),
            F.lit("MISSING_REQUIRED_FIELD").alias("error_type"),
            F.lit("One or more required fields are null").alias("error_message"),
            F.to_json(F.struct("*")).alias("raw_record"),
        )
    )

    clean_df = agent_details_df.filter(~error_condition)

    # -------------------------------------------------
    # 2. DEFENSIVE COLUMNS
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
    # 3. NORMALIZE → FINAL AGENTS SHAPE
    # -------------------------------------------------
    all_agents_df = clean_df.select(
        # PK
        F.col("id").cast("long").alias("agent_id"),

        # contact.*
        F.col("contact.email").alias("email"),
        F.col("contact.name").alias("name"),
        F.col("contact.job_title").alias("job_title"),
        F.col("contact.language").alias("language"),
        F.col("contact.mobile").alias("mobile"),
        F.col("contact.phone").alias("phone"),
        F.col("contact.time_zone").alias("time_zone"),
        F.col("contact.last_login_at").cast("timestamp").alias("last_login_at"),

        # availability
        F.col("available"),
        F.col("available_since").cast("timestamp"),

        # status flags
        F.col("deactivated"),
        F.col("focus_mode"),
        F.col("agent_operational_status"),

        # timestamps
        F.col("last_active_at").cast("timestamp"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp"),

        # org / misc
        F.col("org_agent_id"),
        F.col("ticket_scope"),
        F.col("signature"),
        F.col("freshchat_agent"),
    )

    # -------------------------------------------------
    # 4. ACTIVE AGENTS
    # -------------------------------------------------
    active_agents_df = (
        agents_df
        .select(F.col("id").cast("long").alias("agent_id"))
        .distinct()
        .withColumn("is_active", F.lit(True))
    )

    # -------------------------------------------------
    # 5. DERIVE STATUS
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
