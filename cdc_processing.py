import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("glue-s3-tables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "s3tablesbucket") \
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.warehouse", "arn:aws:s3tables:us-east-1:131578276461:bucket/datalake-example-s3tables") \
    .getOrCreate()

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'table_name'])


########## Auxiliar methods
def handle_schema_evolution(existing_df, cdc_df, column_mapping=None):
    """
    Comprehensive schema evolution handler
    """
    if column_mapping is None:
        column_mapping = {}

    # Apply column renaming to existing data
    for old_col, new_col in column_mapping.items():
        if old_col in existing_df.columns:
            existing_df = existing_df.withColumnRenamed(old_col, new_col)

    existing_schema = existing_df.schema
    cdc_schema = cdc_df.schema

    # Add new columns to existing data
    for field in cdc_schema.fields:
        if field.name not in existing_schema.fieldNames():
            existing_df = existing_df.withColumn(field.name, lit(None).cast(field.dataType))

    # Add missing columns to CDC data
    for field in existing_schema.fields:
        if field.name not in cdc_schema.fieldNames() and field.name not in column_mapping.values():
            cdc_df = cdc_df.withColumn(field.name, lit(None).cast(field.dataType))

    # Align column order
    final_columns = list(set(existing_df.columns) | set(cdc_df.columns))
    existing_df = existing_df.select(*[c for c in final_columns if c in existing_df.columns])
    cdc_df = cdc_df.select(*[c for c in final_columns if c in cdc_df.columns])

    return existing_df, cdc_df
######################################################################

# Read CDC parquet files
cdc_path = "/2023/01/02/13/cdc-001.parquet"
source_path = f"s3://{args['source_bucket']}/people/{cdc_path}"
cdc_df = spark.read.parquet(source_path)


# Read existing table
table_name = f"s3tablesmarcos.{args['table_name']}"
existing_df = spark.read.format("iceberg").table(table_name)

# Usage in your code:
existing_df, cdc_df = handle_schema_evolution(
    existing_df,
    cdc_df,
    column_mapping={"old_address": "address"}  # Optional renames
)

# Separate CDC operations
inserts = cdc_df.filter(col("Op") == "I").drop("Op")
updates = cdc_df.filter(col("Op") == "U").drop("Op")
deletes = cdc_df.filter(col("Op") == "D")

# Apply inserts
if inserts.count() > 0:
    inserts.write \
        .format("iceberg") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .saveAsTable(table_name)

# Apply updates using merge
if updates.count() > 0:
    updates.createOrReplaceTempView("updates_temp")

    spark.sql(f"""
        MERGE INTO {table_name} target
        USING updates_temp source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
    """)

# Apply deletes
if deletes.count() > 0:
    deletes.createOrReplaceTempView("deletes_temp")

    spark.sql(f"""
        MERGE INTO {table_name} target
        USING deletes_temp source
        ON target.id = source.id
        WHEN MATCHED THEN DELETE
    """)

print(f"CDC processing completed for table {table_name}")