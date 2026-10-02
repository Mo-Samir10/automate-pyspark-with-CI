import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data

@pytest.fixture(scope="module")
def spark_local():
    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield spark
    spark.stop()

def test_clean_data_logic(spark_local):

    data = [
        ("1001", "2026-09-01", "Alice", 150.0),
        ("1002", "2026-09-02", "Bob", -20.0),
        ("1003", "2026-09-03", "Sara", 0.0),
        ("1004", "2026-09-04", None, 200.0), 
    ]

    df = spark_local.createDataFrame(
        data,
        ["order_id", "order_Date", "customer_name", "amount"]
    )

    transformed_df = clean_data(df)

    results = transformed_df.collect()

    assert len(results) == 1
    assert results[0]["order_id"] == "1001"
    assert results[0]["customer_name"] == "Alice"
    assert results[0]["amount"] == 150.0
    
    assert results[0]["amount_with_tax"] == 180.0