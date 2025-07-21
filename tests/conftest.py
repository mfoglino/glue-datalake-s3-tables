import pytest
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

S3_TABLE_BUCKET = "arn:aws:s3tables:us-east-1:123456789012:bucket/datalake-example-s3tables"


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("test") \
        .config("spark.sql.warehouse.dir", "/tmp/warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", "s3tablesbucket") \
        .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
        .config("spark.sql.catalog.s3tablesbucket.warehouse", S3_TABLE_BUCKET) \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/warehouse") \
        .getOrCreate()
    
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def glue_context(spark):
    sc = spark.sparkContext
    return GlueContext(sc)