"""Общее для streaming- и batch-задач: сессия Spark, схема события, доступ к PostgreSQL.

Обе задачи используют одну схему и одно правило разбора события — это снимает
главный минус Lambda (два разных кода, которые расходятся).
"""
import os
import time

import boto3
import psycopg2
from botocore.exceptions import ClientError, EndpointConnectionError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

PG_HOST = os.environ["PG_HOST"]
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("sensor_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
])


def ensure_bucket(name="lake"):
    """Ждёт, пока MinIO начнёт отвечать, и создаёт бакет, если его ещё нет."""
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                      region_name=os.getenv("AWS_REGION", "us-east-1"))
    for _ in range(60):
        try:
            s3.head_bucket(Bucket=name)
            return
        except EndpointConnectionError:
            time.sleep(2)          # MinIO ещё стартует
        except ClientError:
            try:
                s3.create_bucket(Bucket=name)
                print(f"bucket {name} created", flush=True)
                return
            except ClientError as e:
                if e.response["Error"]["Code"] in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                    return
                raise
    raise RuntimeError("MinIO is not reachable")


def spark_session(app_name):
    """Spark + каталог Iceberg `lake`: метаданные в PostgreSQL (JDBC), файлы в MinIO."""
    ensure_bucket()
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "jdbc")
        .config("spark.sql.catalog.lake.uri", f"jdbc:postgresql://{PG_HOST}:5432/iceberg_catalog")
        .config("spark.sql.catalog.lake.jdbc.user", PG_USER)
        .config("spark.sql.catalog.lake.jdbc.password", PG_PASSWORD)
        .config("spark.sql.catalog.lake.jdbc.schema-version", "V1")
        .config("spark.sql.catalog.lake.warehouse", "s3://lake/warehouse")
        .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lake.s3.endpoint", os.environ["S3_ENDPOINT"])
        .config("spark.sql.catalog.lake.s3.path-style-access", "true")
        .getOrCreate()
    )


def parse_events(df, payload_col="payload"):
    """JSON-строка -> типизированное событие. Битые сообщения отсекаются здесь,
    но в bronze они остаются в исходном виде."""
    return (
        df.withColumn("e", F.from_json(F.col(payload_col), EVENT_SCHEMA))
        .select("e.*", *[c for c in df.columns if c != payload_col])
        .where(F.col("event_id").isNotNull() & F.col("event_time").isNotNull())
    )


def pg_connect():
    return psycopg2.connect(host=PG_HOST, dbname="serving", user=PG_USER, password=PG_PASSWORD)
