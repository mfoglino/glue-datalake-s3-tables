import pytest
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

S3_TABLE_BUCKET = "arn:aws:s3tables:us-east-1:131578276461:bucket/datalake-example-s3tables"

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder \
        .appName("test") \
         .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160") \
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.jars", "/home/hadoop/workspace/jars/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar") \
        .config("spark.sql.defaultCatalog", "s3tablesbucket") \
        .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
        .config("spark.sql.catalog.s3tablesbucket.warehouse", S3_TABLE_BUCKET) \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.s3tables.glue.id", "131578276461:s3tablescatalog/datalake-example-s3tables") \
        .getOrCreate())
    
    yield spark
    spark.stop()




@pytest.fixture(scope="session")
def glue_context(spark):
    sc = spark.sparkContext
    return GlueContext(sc)