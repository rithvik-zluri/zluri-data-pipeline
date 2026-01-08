from pyspark.sql.functions import col, explode
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader
import argparse
import os


def run_agents_pipeline(day: str):
    data_path = os.path.join("sample_data", f"sync-{day}", "agent_details")

    spark = get_spark_session("agents-pipeline")
    reader = DataReader(spark)

    print(f"Reading agents data from: {data_path}")
    df = reader.read(data_path, file_format="json")

    print("Raw Schema:")
    df.printSchema()

    # =========================
    # AGENTS TABLE
    # =========================
    agents_df = df.select(
        col("id").alias("agent_id"),
        col("contact.email").alias("email"),
        col("contact.name").alias("name"),
        col("contact.job_title").alias("job_title"),
        col("contact.language").alias("language"),
        col("contact.mobile").alias("mobile"),
        col("contact.phone").alias("phone"),
        col("contact.time_zone").alias("time_zone"),
        col("available"),
        col("deactivated"),
        col("focus_mode"),
        col("agent_operational_status"),
        col("last_active_at").cast("timestamp"),
        col("created_at").cast("timestamp"),
        col("updated_at").cast("timestamp")
    )

    # =========================
    # AGENT DETAILS TABLE
    # =========================
    agent_details_df = df.select(
        col("id").alias("agent_id"),
        col("org_agent_id"),
        col("ticket_scope"),
        col("signature"),
        col("freshchat_agent"),
        col("contact.active").alias("is_active"),
        col("contact.avatar").alias("avatar"),
        col("contact.last_login_at").alias("last_login_at"),
        col("created_at").cast("timestamp"),
        col("updated_at").cast("timestamp")
    )

    # =========================
    # AGENT AVAILABILITY (FLATTENED)
    # =========================
    availability_df = df.select(
        col("id").alias("agent_id"),
        explode(col("availability")).alias("availability")
    ).select(
        col("agent_id"),
        col("availability.available").alias("is_available"),
        col("availability.available_since").alias("available_since"),
        col("availability.channel").alias("channel")
    )

    print("Agents Schema:")
    agents_df.printSchema()

    print("Agent Details Schema:")
    agent_details_df.printSchema()

    print("Availability Schema:")
    availability_df.printSchema()

    # =========================
    # WRITE TO POSTGRES (STAGING TABLES)
    # =========================
    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"
    db_properties = {
        "user": "rithvik_zluri_pipeline_user",
        "password": "rithvik_zluri_pipeline_pass",
        "driver": "org.postgresql.Driver"
    }

    agents_df.write.mode("overwrite").jdbc(jdbc_url, "stg_agents", properties=db_properties)
    agent_details_df.write.mode("overwrite").jdbc(jdbc_url, "stg_agent_details", properties=db_properties)
    availability_df.write.mode("overwrite").jdbc(jdbc_url, "stg_agent_availability", properties=db_properties)

    print(f"✅ Agents staging load completed successfully for {day}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="sync day (e.g. day1, day2)")
    args = parser.parse_args()

    run_agents_pipeline(args.day)
