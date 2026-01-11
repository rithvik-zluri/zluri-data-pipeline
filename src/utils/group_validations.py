from pyspark.sql.functions import col, lit, to_json, struct


def validate_group_schema(raw_df):
    """
    Validate mandatory fields: id, name
    """
    error_df = raw_df.filter(
        col("id").isNull() | col("name").isNull()
    ).select(
        col("id").alias("group_id"),
        lit("SCHEMA_ERROR").alias("error_type"),
        lit("id or name is null").alias("error_message"),
        to_json(struct("*")).alias("raw_record")
    )

    clean_df = raw_df.filter(
        col("id").isNotNull() & col("name").isNotNull()
    )

    return clean_df, error_df


def validate_parent_relationship(groups_df):
    """
    parent_group_id must exist and not self reference
    """
    invalid_parent_df = groups_df.filter(
        (col("parent_group_id").isNotNull()) &
        (
            (col("parent_group_id") == col("group_id"))
        )
    )

    valid_df = groups_df.subtract(invalid_parent_df)

    error_df = invalid_parent_df.select(
        col("group_id"),
        lit("HIERARCHY_ERROR").alias("error_type"),
        lit("parent_group_id is same as group_id").alias("error_message")
    )

    return valid_df, error_df


def validate_membership(membership_df, agents_df):
    """
    agent_id must exist in agents table
    """
    valid_membership_df = membership_df.join(
        agents_df.select("agent_id"),
        on="agent_id",
        how="inner"
    )

    invalid_membership_df = membership_df.join(
        agents_df.select("agent_id"),
        on="agent_id",
        how="left_anti"
    )

    error_df = invalid_membership_df.select(
        col("group_id"),
        col("agent_id"),
        lit("INVALID_AGENT_REFERENCE").alias("error_type"),
        lit("agent_id not found in agents table").alias("error_message")
    )

    return valid_membership_df, error_df
