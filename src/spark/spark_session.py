from pyspark.sql import SparkSession
import os


def get_spark_session(app_name: str = "zluri-data-pipeline") -> SparkSession:
    """
    SparkSession for:
    - Local filesystem
    - Public S3 buckets (anonymous access)
    - Large datasets via s3a://

    Compatible with Spark 4.x + Hadoop 3.3.x
    """

    os.environ.setdefault("JAVA_HOME", os.environ.get("JAVA_HOME", ""))

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # --------------------
        # Memory (safe defaults)
        # --------------------
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")

        # Shuffle tuning (IMPORTANT for large joins)
        .config("spark.sql.shuffle.partitions", "200")

        # --------------------
        # Dependencies
        # --------------------
        .config(
            "spark.jars.packages",
            ",".join([
                "org.postgresql:postgresql:42.6.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ])
        )

        # --------------------
        # S3A – public bucket
        # --------------------
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

        # --------------------
        # 🚀 Large S3 dataset tuning (CRITICAL)
        # --------------------
        # Fast uploads
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")

        # Multipart tuning (128 MB parts)
        .config("spark.hadoop.fs.s3a.multipart.size", "134217728")
        .config("spark.sql.files.maxPartitionBytes", "134217728")

        # Connection & threading
        .config("spark.hadoop.fs.s3a.connection.maximum", "256")
        .config("spark.hadoop.fs.s3a.threads.max", "256")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")

        # Timeouts
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")

        # Retry safety
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .config("spark.hadoop.fs.s3a.retry.limit", "3")
        .config("spark.hadoop.fs.s3a.retry.interval", "1000")

        # Multipart cleanup safety
        .config("spark.hadoop.fs.s3a.multipart.purge", "false")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")

        # --------------------
        # Spark stability
        # --------------------
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.broadcastTimeout", "1200")

        .getOrCreate()
    )

    return spark
