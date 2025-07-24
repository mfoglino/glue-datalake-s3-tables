import boto3
from typing import List, Optional, Dict
from pyspark.sql.types import StructType

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