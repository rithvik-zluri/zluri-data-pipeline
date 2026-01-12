# src/pipelines/agent_roles/agent_roles_transform.py

from pyspark.sql.functions import col, when

def transform_agent_roles(agents_df, roles_df):
    """
    Core transformation logic for agent-roles pipeline.
    This is what we unit test.
    """

    # ---------------------------
    # Normalize agents
    # ---------------------------
    agents_clean = agents_df.select(
        col("id").cast("long").alias("agent_id"),
        col("type").alias("agent_type_str")
    ).withColumn(
        "agent_type",
        when(col("agent_type_str") == "support_agent", 1)
        .otherwise(3)  # collaborators, etc.
    )

    # ---------------------------
    # Normalize roles
    # ---------------------------
    roles_clean = roles_df.select(
        col("id").cast("long").alias("role_id"),
        col("agent_type").cast("int")
    )

    # ---------------------------
    # Join agents to roles
    # ---------------------------
    agent_roles_df = agents_clean.join(
        roles_clean,
        on="agent_type",
        how="inner"
    ).select(
        col("agent_id"),
        col("role_id")
    )

    return agent_roles_df
