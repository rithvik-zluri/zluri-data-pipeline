# src/pipelines/groups/groups_transform.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


def transform_groups(raw_df: DataFrame, agents_df: DataFrame):
    """
    Core transformation logic for groups pipeline.

    Returns:
        groups_df               -> clean group dimension
        valid_membership_df     -> valid group-agent links
        error_df                -> invalid or malformed records
    """

    # =====================================================
    # 1. NORMALIZE GROUPS (schema-safe)
    # =====================================================
    groups_df = raw_df.select(
        F.col("id").cast("bigint").alias("group_id"),
        F.col("name"),
        F.col("parent_group_id").cast("bigint"),
        F.to_timestamp(F.col("created_at")).alias("created_at"),
        F.to_timestamp(F.col("updated_at")).alias("updated_at"),
    ).filter(
        F.col("group_id").isNotNull()
    )

    # =====================================================
    # 2. SAFELY HANDLE agent_ids (schema drift tolerant)
    # =====================================================

    # Check if agent_ids exists and is an array
    agent_ids_col = (
        F.col("agent_ids")
        if "agent_ids" in raw_df.columns
        and isinstance(raw_df.schema["agent_ids"].dataType, ArrayType)
        else F.array()
    )

    exploded_df = raw_df.select(
        F.col("id").cast("bigint").alias("group_id"),
        F.explode_outer(agent_ids_col).alias("agent_id")
    )

    membership_df = exploded_df.select(
        F.col("group_id"),
        F.col("agent_id").cast("bigint").alias("agent_id")
    )

    # =====================================================
    # 3. SPLIT VALID / INVALID MEMBERSHIPS
    # =====================================================
    valid_membership_df = membership_df.join(
        agents_df.select(F.col("agent_id")),
        on="agent_id",
        how="inner"
    )

    invalid_membership_df = membership_df.filter(
        F.col("agent_id").isNotNull()
    ).join(
        agents_df.select(F.col("agent_id")),
        on="agent_id",
        how="left_anti"
    )

    # =====================================================
    # 4. ERROR RECORDS (schema-safe JSON)
    # =====================================================
    error_df = invalid_membership_df.select(
        F.col("group_id"),
        F.lit("INVALID_AGENT_ID").alias("error_type"),
        F.lit("agent_id does not exist in agents table").alias("error_message"),
        F.to_json(
            F.struct(
                F.col("group_id").alias("group_id"),
                F.col("agent_id").alias("agent_id")
            )
        ).alias("raw_record")
    )

    return groups_df, valid_membership_df, error_df
