# src/pipelines/roles/roles_transform.py

from pyspark.sql import functions as F
from pyspark.sql.types import StructType


# -------------------------------------------------
# UTILS
# -------------------------------------------------
def safe_col(df, col_name):
    if col_name in df.columns:
        return F.col(col_name)
    return F.lit(None)


def safe_nested_col(df, struct_col, nested_col):
    if struct_col in df.columns:
        field = df.schema[struct_col].dataType
        if isinstance(field, StructType) and nested_col in field.names:
            return F.col(f"{struct_col}.{nested_col}")
    return F.lit(None)


def safe_coalesce(df, nested_path, flat_col):
    struct_col, nested_col = nested_path.split(".")
    return F.coalesce(
        safe_nested_col(df, struct_col, nested_col),
        safe_col(df, flat_col),
    )


# -------------------------------------------------
# TRANSFORM
# -------------------------------------------------
def transform_roles(roles_df):
    """
    Schema-drift tolerant roles transform.
    Supports:
    - Flat JSON
    - Nested role.* JSON
    """

    # -------------------------------------------------
    # 0. CAPTURE RAW RECORD (SAFE)
    # -------------------------------------------------
    raw_df = roles_df.withColumn(
        "_raw_record",
        F.to_json(F.struct(*[F.col(c) for c in roles_df.columns]))
    )

    # -------------------------------------------------
    # 1. NORMALIZE
    # -------------------------------------------------
    normalized_df = raw_df.select(
        F.expr("try_cast(id as bigint)").alias("role_id"),

        safe_coalesce(raw_df, "role.name", "name").alias("name"),
        safe_coalesce(raw_df, "role.description", "description").alias("description"),
        safe_coalesce(raw_df, "role.default", "default").alias("is_default"),
        safe_coalesce(raw_df, "role.agent_type", "agent_type").alias("agent_type"),

        F.to_timestamp(
            safe_coalesce(raw_df, "role.created_at", "created_at")
        ).alias("created_at"),

        F.to_timestamp(
            safe_coalesce(raw_df, "role.updated_at", "updated_at")
        ).alias("updated_at"),

        F.col("_raw_record"),
    )

    # -------------------------------------------------
    # 2. REQUIRED FIELD VALIDATION
    # -------------------------------------------------
    error_condition = (
        F.col("role_id").isNull()
        | F.col("name").isNull()
    )

    error_df = (
        normalized_df
        .filter(error_condition)
        .select(
            F.col("role_id"),
            F.lit("MISSING_REQUIRED_FIELD").alias("error_type"),
            F.lit("Role id or name is null").alias("error_message"),
            F.col("_raw_record").alias("raw_record"),
        )
    )

    clean_df = normalized_df.filter(~error_condition)

    # -------------------------------------------------
    # 3. FINAL SHAPE
    # -------------------------------------------------
    final_df = clean_df.select(
        "role_id",
        "name",
        "description",
        "is_default",
        "agent_type",
        "created_at",
        "updated_at",
    )

    return final_df, error_df
