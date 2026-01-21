# src/pipelines/agent_roles/agent_roles_transform.py

from pyspark.sql.functions import col, explode, lit, to_json, struct

def transform_agent_roles(agent_details_df, roles_df):
    """
    Transforms agent_details to extract agent-role mappings.
    Validates against roles_df.
    Returns (valid_df, error_df).
    """

    # 1. Explode agent roles
    exploded_agents = agent_details_df.select(
        col("id").cast("long").alias("agent_id"),
        explode(col("role_ids")).alias("role_id"),
        col("role_ids").alias("original_role_ids") # Keep for debugging/error context if needed
    ).select(
        col("agent_id"),
        col("role_id").cast("long").alias("role_id")
    )

    # 2. Prepare roles for validation
    valid_roles = roles_df.select(
        col("id").cast("long").alias("role_id")
    ).distinct()

    # 3. Join to validate
    # Left join to keep all agent-role attempts, then check if found
    joined_df = exploded_agents.join(
        valid_roles.withColumn("is_valid", lit(True)),
        on="role_id",
        how="left"
    )

    # 4. Separate Valid vs Invalid
    
    # Valid records: have is_valid = True
    valid_df = joined_df.filter(col("is_valid") == True).select(
        col("agent_id"),
        col("role_id")
    )

    # Error records: is_valid is Null
    # We construct the error record format: role_id, error_type, error_message, raw_record
    error_df = joined_df.filter(col("is_valid").isNull()).select(
        col("role_id"),
        lit("INVALID_ROLE_ID").alias("error_type"),
        lit("Role ID not found in roles master").alias("error_message"),
        to_json(struct(col("agent_id"), col("role_id"))).alias("raw_record")
    )

    return valid_df, error_df
