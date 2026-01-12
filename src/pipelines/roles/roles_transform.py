# src/pipelines/roles/roles_transform.py

from pyspark.sql.functions import col, lit, to_json, struct

def transform_roles(roles_df):
    """
    Core transformation logic for roles pipeline.
    This is what we unit test.
    """

    # -----------------------------------
    # ERROR DETECTION – missing critical fields
    # -----------------------------------
    error_df = roles_df.filter(
        col("id").isNull() |
        col("name").isNull()
    ).select(
        col("id").alias("role_id"),
        lit("MISSING_REQUIRED_FIELD").alias("error_type"),
        lit("Role id or name is null").alias("error_message"),
        to_json(struct("*")).alias("raw_record")
    )

    # -----------------------------------
    # CLEAN DATA ONLY
    # -----------------------------------
    clean_df = roles_df.filter(
        col("id").isNotNull() &
        col("name").isNotNull()
    )

    # -----------------------------------
    # NORMALIZE FIELDS
    # -----------------------------------
    final_df = clean_df.select(
        col("id").cast("long").alias("role_id"),
        col("name"),
        col("description"),
        col("default").alias("is_default"),
        col("agent_type"),
        col("created_at"),
        col("updated_at")
    )

    return final_df, error_df
