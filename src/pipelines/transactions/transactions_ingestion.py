import os
from pyspark.sql import SparkSession


def read_transactions(spark: SparkSession, day: str):
    base_path = os.path.join("sample_data", f"sync-{day}", "transactions")
    print(f"📥 Reading transactions from: {base_path}")

    df = (
        spark.read
        .option("multiline", "true")
        .json(base_path)
    )

    return df
