import argparse

from src.db.connection import get_postgres_properties
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader
from src.pipelines.groups.groups_ingestion import (
    read_groups_raw,
    read_agents_from_db
)
from src.pipelines.groups.groups_transform import transform_groups


def run_groups_pipeline(day: str):
    print(f"=== Starting groups pipeline for {day} ===")

    spark = get_spark_session("groups-pipeline")
    reader = DataReader(spark)

    # -----------------------------------
    # INGESTION
    # -----------------------------------
    raw_df = read_groups_raw(reader, day)

    db_properties = get_postgres_properties()
    jdbc_url = db_properties["url"]

    agents_df = read_agents_from_db(spark, jdbc_url, db_properties)


    # -----------------------------------
    # TRANSFORM
    # -----------------------------------
    groups_df, valid_membership_df, error_df = transform_groups(raw_df, agents_df)




    # -----------------------------------
    # WRITE stg_groups
    # -----------------------------------
    groups_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "stg_groups") \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .mode("overwrite") \
        .save()

    print("Data written to stg_groups")

    # -----------------------------------
    # WRITE stg_group_membership (ONLY VALID)
    # -----------------------------------
    valid_membership_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "stg_group_membership") \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .mode("overwrite") \
        .save()

    print("Valid data written to stg_group_membership")

    # -----------------------------------
    # WRITE ERRORS
    # -----------------------------------
    error_count = error_df.count()

    if error_count > 0:
        error_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "group_pipeline_errors") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .option("stringtype", "unspecified") \
            .mode("append") \
            .save()

    print(f"Error records written: {error_count}")
    print(f"Groups pipeline completed successfully for {day}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day folder, e.g. day1, day2")
    args = parser.parse_args()

    run_groups_pipeline(args.day)
