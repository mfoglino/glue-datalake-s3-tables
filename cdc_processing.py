import sys
from datetime import datetime
from typing import Optional, List

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType
from awsglue.context import GlueContext

spark = SparkSession.builder.appName("glue-s3-tables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "s3tablesbucket") \
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.warehouse", "arn:aws:s3tables:us-east-1:131578276461:bucket/datalake-example-s3tables") \
    .getOrCreate()

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'table_name'])

# Build GlueContext from the SparkSession
glueContext = GlueContext(spark.sparkContext)
logger = glueContext.get_logger()


# ----------------------------------------------------------------------------------------------------------------------
# Auxiliar methods
# ----------------------------------------------------------------------------------------------------------------------

def get_checkpoint_from_dynamodb(table_name: str) -> Optional[str]:
    """Get last processed CDC file from DynamoDB"""
    try:
        dynamodb = boto3.resource('dynamodb')
        checkpoint_table = dynamodb.Table('etl_table_checkpoints')

        response = checkpoint_table.get_item(
            Key={'table_name': table_name}
        )

        if 'Item' in response:
            return response['Item'].get('last_processed_file')
    except Exception as e:
        print(f"Error reading checkpoint from DynamoDB: {e}")
    return None

def update_checkpoint_in_dynamodb(table_name: str, last_processed_file: str):
    """Update checkpoint in DynamoDB"""
    try:
        dynamodb = boto3.resource('dynamodb')
        checkpoint_table = dynamodb.Table('etl_table_checkpoints')

        checkpoint_table.put_item(
            Item={
                'table_name': table_name,
                'last_processed_file': last_processed_file,
                'updated_at': datetime.now().isoformat()
            }
        )
        print(f"Updated checkpoint in DynamoDB: {last_processed_file}")
    except Exception as e:
        print(f"Error updating checkpoint in DynamoDB: {e}")

def list_cdc_files(bucket: str) -> List[str]:
    """List all CDC files in chronological order"""
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix="people/"
        )

        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet') and 'cdc-' in obj['Key']:
                    files.append(obj['Key'])

        # Sort by modification time or file name pattern
        return sorted(files)
    except Exception as e:
        print(f"Error listing CDC files: {e}")
        return []

def get_unified_schema(file_paths: List[str], bucket: str) -> StructType:
    """Get unified schema by reading all CDC files and merging schemas"""
    unified_schema = None

    for file_path in file_paths:
        try:
            full_path = f"s3://{bucket}/{file_path}"
            df = spark.read.parquet(full_path)
            current_schema = df.schema

            if unified_schema is None:
                unified_schema = current_schema
            else:
                # Merge schemas by adding missing fields
                unified_fields = {f.name: f for f in unified_schema.fields}

                for field in current_schema.fields:
                    if field.name not in unified_fields:
                        # Add new field to unified schema
                        unified_schema = unified_schema.add(field)
                        unified_fields[field.name] = field
                        print(f"Added field {field.name} to unified schema from {file_path}")
        except Exception as e:
            print(f"Error reading schema from {file_path}: {e}")
            continue

    return unified_schema

def normalize_dataframe_to_schema(df, target_schema: StructType, source_file: str):
    """Normalize a dataframe to match the target schema"""
    current_fields = {f.name: f for f in df.schema.fields}

    # Add missing columns with null values
    for field in target_schema.fields:
        if field.name not in current_fields:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
            print(f"Added missing column {field.name} to {source_file}")

    # Select columns in the same order as target schema
    column_order = [field.name for field in target_schema.fields]
    df = df.select(*column_order)

    return df

def read_and_union_cdc_files(bucket: str, file_paths: List[str]):
    """Read multiple CDC files with different schemas and union them"""
    if not file_paths:
        print("No CDC files to process")
        return None

    print(f"Processing {len(file_paths)} CDC files")

    # First pass: get unified schema
    print("Analyzing schemas across all CDC files...")
    unified_schema = get_unified_schema(file_paths, bucket)

    if unified_schema is None:
        print("Could not determine unified schema")
        return None

    print(f"Unified schema has {len(unified_schema.fields)} fields")

    # Second pass: read and normalize all files
    dataframes = []
    for file_path in file_paths:
        try:
            full_path = f"s3://{bucket}/{file_path}"
            df = spark.read.parquet(full_path)

            # Normalize to unified schema
            df = normalize_dataframe_to_schema(df, unified_schema, file_path)

            # Add metadata column
            df = df.withColumn("_source_file", lit(file_path))

            dataframes.append(df)
            print(f"Normalized and loaded: {file_path} with {df.count()} records")

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            continue

    if not dataframes:
        print("No valid dataframes to union")
        return None

    # Union all normalized dataframes
    result_df = dataframes[0]
    for df in dataframes[1:]:
        result_df = result_df.union(df)

    print(f"Successfully unioned {len(dataframes)} files with {result_df.count()} total records")
    return result_df

def get_new_cdc_files(bucket: str, last_checkpoint: Optional[str]) -> List[str]:
    """Get list of new CDC files to process"""
    all_cdc_files = list_cdc_files(bucket)

    if not last_checkpoint:
        return all_cdc_files

    try:
        # Find files after the checkpoint
        checkpoint_index = all_cdc_files.index(last_checkpoint)
        return all_cdc_files[checkpoint_index + 1:]
    except ValueError:
        print(f"Checkpoint file {last_checkpoint} not found, processing all files")
        return all_cdc_files

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
# ----------------------------------------------------------------------------------------------------------------------
# Auxiliar methods end
# ----------------------------------------------------------------------------------------------------------------------



if __name__ == "__main__":
    # Read CDC parquet files
    table_name = f"s3tablesmarcos.{args['table_name']}"

    # Get last checkpoint from DynamoDB
    last_checkpoint = get_checkpoint_from_dynamodb(args['table_name'])
    logger.info(f"Last checkpoint from DynamoDB: {last_checkpoint}")

    # Get new CDC files to process
    new_cdc_files = get_new_cdc_files(args['source_bucket'], last_checkpoint)
    logger.info(f"New CDC files to process: {new_cdc_files}")

    if not new_cdc_files:
        logger.info("No new CDC files to process")
        exit(1)

    logger.info(f"Found {len(new_cdc_files)} new CDC files to process")

    # Read and union all new CDC files with schema normalization
    cdc_df = read_and_union_cdc_files(args['source_bucket'], new_cdc_files)

    if cdc_df is None:
        logger.info("No valid CDC data to process")
        exit(1)

    # Continue with existing processing...
    existing_df = spark.read.format("iceberg").table(table_name)

    existing_df, cdc_df, table_columns = handle_schema_evolution_iceberg(
        existing_df,
        cdc_df,
        table_name,
        column_mapping={}
    )

    # Process CDC operations (your existing code)
    inserts = cdc_df.filter(col("op") == "I").select(*table_columns)
    updates = cdc_df.filter(col("op") == "U").select(*table_columns)
    deletes = cdc_df.filter(col("op") == "D")

    # Apply operations...
    if inserts.count() > 0:
        inserts.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable(table_name)

    if updates.count() > 0:
        updates.createOrReplaceTempView("updates_temp")
        spark.sql(f"""
            MERGE INTO {table_name} target
            USING updates_temp source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET *
        """)

    if deletes.count() > 0:
        deletes.select("id").createOrReplaceTempView("deletes_temp")
        spark.sql(f"""
            MERGE INTO {table_name} target
            USING deletes_temp source
            ON target.id = source.id
            WHEN MATCHED THEN DELETE
        """)

    # Update checkpoint in DynamoDB
    if new_cdc_files:
        logger.info(f"Updating checkpoint in DynamoDB: {new_cdc_files[-1]}")
        update_checkpoint_in_dynamodb(args['table_name'], new_cdc_files[-1])

    logger.info(f"CDC processing completed for table {table_name}")
