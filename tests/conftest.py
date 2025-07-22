import pytest
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("test") \
        .getOrCreate()
    
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def glue_context(spark):
    sc = spark.sparkContext
    return GlueContext(sc)