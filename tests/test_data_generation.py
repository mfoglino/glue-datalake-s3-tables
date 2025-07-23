
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

def test_generate_initial_data(spark, glue_context):

    # Generate initial people data
    initial_data = [
        (1, "John", "Doe", "john.doe@email.com", datetime(2023, 1, 1), "I"),
        (2, "Jane", "Smith", "jane.smith@email.com", datetime(2023, 1, 1), "I"),
        (3, "Bob", "Johnson", "bob.johnson@email.com", datetime(2023, 1, 1), "I")
    ]
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("Op", StringType(), True)
    ])
    
    df = spark.createDataFrame(initial_data, schema)
    
    # Write initial load file to S3 DMS landing bucket
    df.coalesce(1).write.mode("overwrite").parquet("s3://datalake-example-landing/people/LOAD00000001.parquet")
    


def test_generate_cdc_data(spark, glue_context):
    # Generate CDC data
    cdc_data = [
        (4, "Alice", "Brown", "alice.brown@email.com", datetime(2023, 1, 2), "I", "123 Main St"),
        (2, "Jane", "Wilson", "jane.wilson@email.com", datetime(2023, 1, 2), "U", "456 Oak Ave"),
        (3, None, None, None, None, "D", None)
    ]
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("Op", StringType(), True),
        StructField("address", StringType(), True)
    ])
    
    cdc_df = spark.createDataFrame(cdc_data, schema)
    
    # Write CDC file to S3 DMS landing bucket
    cdc_df.coalesce(1).write.mode("overwrite").parquet("s3://datalake-example-landing/people/2023/01/02/13/cdc-001.parquet")
    
    assert cdc_df.count() == 3