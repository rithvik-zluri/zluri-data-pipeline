from pyspark.sql import SparkSession
import os

def get_spark_session(app_name="zluri-data-pipeline"):
    os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME", "")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )

    return spark
