import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source-bucket', 'table-namespace', 'table-name', 'warehouse-path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read initial load parquet file
source_path = f"s3://{args['source_bucket']}/landing/people/LOAD00000001.parquet"
df = spark.read.parquet(source_path)

# Configure Spark for S3 Tables
spark.conf.set("spark.sql.catalog.s3tablesbucket.warehouse", args['warehouse_path'])

# Write to S3 Tables
table_name = f"{args['table_namespace']}.{args['table_name']}"
df.write.format("iceberg").mode("overwrite").saveAsTable(table_name)

job.commit()