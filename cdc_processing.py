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
def handle_schema_evolution_iceberg(existing_df, cdc_df, table_name, column_mapping=None):
    """
    Iceberg-specific schema evolution handler
    """
    if column_mapping is None:
        column_mapping = {}

    # Apply column renaming to CDC data
    for old_col, new_col in column_mapping.items():
        if old_col in cdc_df.columns:
            cdc_df = cdc_df.withColumnRenamed(old_col, new_col)

    existing_schema = existing_df.schema
    cdc_schema = cdc_df.schema

    # Find new columns in CDC data (excluding op column)
    new_columns = []
    for field in cdc_schema.fields:
        if field.name not in existing_schema.fieldNames() and field.name != "op":
            new_columns.append(field)

    # Add new columns to Iceberg table schema first
    for field in new_columns:
        try:
            spark.sql(f"""
                ALTER TABLE {table_name}
                ADD COLUMN {field.name} {field.dataType.simpleString()}
            """)
            print(f"Added column {field.name} to table {table_name}")
        except Exception as e:
            print(f"Column {field.name} might already exist: {e}")

    # Re-read the table to get updated schema
    existing_df = spark.read.format("iceberg").table(table_name)

    # Get table columns (excluding op)
    table_columns = existing_df.columns

    # Add missing columns to CDC data with null values
    for col_name in table_columns:
        if col_name not in cdc_df.columns:
            # Get the data type from existing schema
            field_type = next((f.dataType for f in existing_df.schema.fields if f.name == col_name), "string")
            cdc_df = cdc_df.withColumn(col_name, lit(None).cast(field_type))

    return existing_df, cdc_df, table_columns
######################################################################

# Read CDC parquet files



cdc_path = "/2023/01/02/13/cdc-001.parquet"
source_path = f"s3://{args['source_bucket']}/people/{cdc_path}"
cdc_df = spark.read.parquet(source_path)


# Read existing table
table_name = f"s3tablesmarcos.{args['table_name']}"
existing_df = spark.read.format("iceberg").table(table_name)

existing_df, cdc_df, table_columns = handle_schema_evolution_iceberg(
    existing_df,
    cdc_df,
    table_name,
    column_mapping={}  # Add any column renames here
)

# Separate CDC operations and select only table columns
inserts = cdc_df.filter(col("op") == "I").select(*table_columns)
updates = cdc_df.filter(col("op") == "U").select(*table_columns)
deletes = cdc_df.filter(col("op") == "D")

# Apply inserts
if inserts.count() > 0:
    inserts.write \
        .format("iceberg") \
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

# Apply deletes (only need key columns for delete)
if deletes.count() > 0:
    deletes.select("id").createOrReplaceTempView("deletes_temp")

    spark.sql(f"""
        MERGE INTO {table_name} target
        USING deletes_temp source
        ON target.id = source.id
        WHEN MATCHED THEN DELETE
    """)

print(f"CDC processing completed for table {table_name}")