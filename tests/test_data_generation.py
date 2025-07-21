
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

def test_generate_initial_data(spark, glue_context):
    catalog_name = "s3tables"
    table_name = "people"
    
    # Generate initial people data
    initial_data = [
        (1, "John", "Doe", "john.doe@email.com", datetime(2023, 1, 1)),
        (2, "Jane", "Smith", "jane.smith@email.com", datetime(2023, 1, 1)),
        (3, "Bob", "Johnson", "bob.johnson@email.com", datetime(2023, 1, 1))
    ]
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ])
    
    df = spark.createDataFrame(initial_data, schema)
    
    # Write initial load file
    df.coalesce(1).write.mode("overwrite").parquet("tests/data/LOAD00000001.parquet")
    
    # Create Iceberg table
    df.write.format("iceberg").mode("overwrite").saveAsTable(f"{catalog_name}.{table_name}")
    
    # Verify data
    result = spark.read.format("iceberg").table(f"{catalog_name}.{table_name}")
    assert result.count() == 3

def test_generate_cdc_data(spark, glue_context):
    # Generate CDC data
    cdc_data = [
        (4, "Alice", "Brown", "alice.brown@email.com", datetime(2023, 1, 2), "I"),
        (2, "Jane", "Wilson", "jane.wilson@email.com", datetime(2023, 1, 2), "U"),
        (3, None, None, None, None, "D")
    ]
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("Op", StringType(), True)
    ])
    
    cdc_df = spark.createDataFrame(cdc_data, schema)
    
    # Write CDC file
    cdc_df.coalesce(1).write.mode("overwrite").parquet("tests/data/cdc-001.parquet")
    
    assert cdc_df.count() == 3