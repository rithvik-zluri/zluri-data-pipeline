from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class DataReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, path: str, file_format: str, multiline: bool = True) -> DataFrame:
        """
        Generic reader for JSON, CSV, Parquet.

        Works with:
        - Local filesystem paths
        - S3 paths (s3a://)
        - Single files or directories
        """

        file_format = file_format.lower()

        if file_format == "json":
            return (
                self.spark.read
                .option("multiline", multiline)
                .option("recursiveFileLookup", "true")
                .json(path)
            )

        elif file_format == "csv":
            return (
                self.spark.read
                .option("header", True)
                .option("inferSchema", True)
                .option("recursiveFileLookup", "true")
                .csv(path)
            )

        elif file_format == "parquet":
            return (
                self.spark.read
                .option("recursiveFileLookup", "true")
                .parquet(path)
            )

        else:
            raise ValueError(f"Unsupported file format: {file_format}")
