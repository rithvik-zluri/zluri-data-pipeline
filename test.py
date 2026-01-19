from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("zluri-public-s3-test")
    # Anonymous access for public S3
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
    )
    # Required to avoid corrupt record analysis issue
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()
)

df = spark.read.json(
    "s3a://zluri-data-assignment/assignment-jan-2026/sync-day1/agents/*.json"
)

df.printSchema()
df.show(5, truncate=False)

spark.stop()
