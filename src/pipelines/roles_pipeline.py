from pyspark.sql.functions import col
from src.spark.spark_session import get_spark_session
from src.utils.reader import DataReader

def run_roles_pipeline(day: str):
    print(f"=== Starting roles pipeline for {day} ===")

    spark = get_spark_session("roles-pipeline")
    reader = DataReader(spark)

    input_path = f"sample_data/sync-{day}/roles"
    print(f"Reading roles from: {input_path}")

    df = reader.read(input_path, "json")

    # Normalize fields
    final_df = df.select(
        col("id").cast("long").alias("role_id"),
        col("name"),
        col("description"),
        col("default").alias("is_default"),
        col("agent_type"),
        col("created_at"),
        col("updated_at")
    )

    print("Sample roles data:")
    final_df.show(truncate=False)

    # DB config (MATCHING agents pipeline)
    jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"
    db_properties = {
        "user": "rithvik_zluri_pipeline_user",
        "password": "rithvik_zluri_pipeline_pass",
        "driver": "org.postgresql.Driver"
    }

    final_df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, "stg_roles", properties=db_properties)

    print("✅ Data written to stg_roles")
    print(f"✅ Roles pipeline completed successfully for {day}")
