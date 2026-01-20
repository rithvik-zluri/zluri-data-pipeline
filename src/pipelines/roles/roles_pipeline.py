# src/pipelines/roles/roles_pipeline.py

import argparse

from src.db.connection import get_postgres_properties
from src.spark.spark_session import get_spark_session
from src.pipelines.roles.roles_ingestion import read_roles
from src.pipelines.roles.roles_transform import transform_roles


def run_roles_pipeline(day: str):
    print(f"=== Starting roles pipeline for {day} ===")

    spark = get_spark_session("roles-pipeline")

    # -----------------------------
    # INGESTION
    # -----------------------------
    roles_df = read_roles(spark, day)

    # -----------------------------
    # TRANSFORMATION
    # -----------------------------
    final_df, error_df = transform_roles(roles_df)


    # -----------------------------------
    # WRITE TO POSTGRES (STAGING)
    # -----------------------------------
    db_properties = get_postgres_properties()
    jdbc_url = db_properties["url"]

    final_df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, "stg_roles", properties=db_properties)

    # -----------------------------------
    # WRITE ERRORS
    # -----------------------------------
    error_count = error_df.count()
    if error_count > 0:
        error_df.write \
            .mode("append") \
            .jdbc(jdbc_url, "role_pipeline_errors", properties=db_properties)

    print(f"Roles pipeline completed successfully for {day}")
    print(f"Error records written: {error_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day (e.g. day1, day2)")
    args = parser.parse_args()

    run_roles_pipeline(args.day)
