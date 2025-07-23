import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from awsglue.context import GlueContext


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'table_namespace', 'table_name'])

spark = SparkSession.builder.appName("glue-s3-tables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "s3tablesbucket") \
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.warehouse", "arn:aws:s3tables:us-east-1:131578276461:bucket/datalake-example-s3tables") \
    .getOrCreate()

# Build GlueContext from the SparkSession
glueContext = GlueContext(spark.sparkContext)
logger = glueContext.get_logger()


# Read initial load parquet file
source_path = f"s3://{args['source_bucket']}/people/LOAD00000001.parquet"
df = spark.read.parquet(source_path)
logger.info(f"Row count: {df.count()}")

# Write to S3 Tables
table_name = f"{args['table_namespace']}.{args['table_name']}"

logger.info(f"Writing to table: {table_name}")
df.writeTo(table_name) \
    .tableProperty("format-version", "2") \
    .createOrReplace()

