# src/pipelines/groups/groups_transform.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, to_timestamp, lit, to_json, struct


def transform_groups(raw_df: DataFrame, agents_df: DataFrame):
    """
    Core transformation logic for groups pipeline.
    Returns:
        groups_df,
        valid_membership_df,
        error_df
    """

    # -----------------------------------
    # GROUPS TABLE (stg_groups)
    # -----------------------------------
    groups_df = raw_df.select(
        col("id").cast("bigint").alias("group_id"),
        col("name"),
        col("parent_group_id").cast("bigint"),
        to_timestamp(col("created_at")).alias("created_at"),
        to_timestamp(col("updated_at")).alias("updated_at")
    )

    # -----------------------------------
    # SAFE EXPLODE agent_ids
    # -----------------------------------
    exploded_df = raw_df.select(
        col("id").cast("bigint").alias("group_id"),
        explode(col("agent_ids")).alias("agent_id")
    )

    membership_df = exploded_df.select(
        col("group_id"),
        col("agent_id").cast("bigint").alias("agent_id")
    )

    # -----------------------------------
    # SPLIT VALID / INVALID MEMBERSHIP
    # -----------------------------------
    valid_membership_df = membership_df.join(
        agents_df,
        on="agent_id",
        how="inner"
    )

    invalid_membership_df = membership_df.join(
        agents_df,
        on="agent_id",
        how="left_anti"
    )

    # -----------------------------------
    # ERROR RECORDS
    # -----------------------------------
    error_df = invalid_membership_df.select(
        col("group_id"),
        lit("INVALID_AGENT_ID").alias("error_type"),
        lit("agent_id does not exist in agents table").alias("error_message"),
        to_json(struct(col("group_id"), col("agent_id"))).alias("raw_record")
    )

    return groups_df, valid_membership_df, error_df
