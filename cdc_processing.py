import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source-bucket', 'table-name', 'cdc-path', 'warehouse-path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read CDC parquet files
source_path = f"s3://{args['source_bucket']}/landing/people/{args['cdc_path']}"
cdc_df = spark.read.parquet(source_path)

# Configure Spark for S3 Tables
spark.conf.set("spark.sql.catalog.s3tablesbucket.warehouse", args['warehouse_path'])

# Read existing table
table_name = f"s3tables.{args['table_name']}"
existing_df = spark.read.format("iceberg").table(table_name)

# Process CDC operations
inserts = cdc_df.filter(col("Op") == "I")
updates = cdc_df.filter(col("Op") == "U") 
deletes = cdc_df.filter(col("Op") == "D")

# Apply changes (simplified merge logic)
if inserts.count() > 0:
    inserts.write.format("iceberg").mode("append").saveAsTable(table_name)

if updates.count() > 0:
    updates.write.format("iceberg").mode("append").saveAsTable(table_name)

job.commit()