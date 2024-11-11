import pytest
from unittest.mock import patch, MagicMock
from utils import get_spark_session, read_from_bigquery, write_to_bigquery
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    # Initialize the Spark session for tests
    return get_spark_session("test_spark_session")

def test_get_spark_session(spark):
    # Verify that a Spark session is successfully created
    assert isinstance(spark, SparkSession), "Failed to create Spark session"

@patch("utils.read_from_bigquery")
def test_read_from_bigquery(mock_read, spark):
    # Mock the DataFrame returned by the read_from_bigquery function
    mock_df = MagicMock()
    mock_df.columns = ["user_id", "user_name"]
    mock_df.count.return_value = 5  # Simulate a non-empty DataFrame
    mock_read.return_value = mock_df
    
    # Call the function (which will now use the mocked version)
    df = read_from_bigquery(spark, "silver", "users")
    
    # Assert that the mock was called and check the DataFrame behavior
    mock_read.assert_called_once_with(spark, "silver", "users")
    assert df.count() > 0, "The DataFrame is empty"
    assert df.columns == ["user_id", "user_name"], "The DataFrame columns do not match"

@patch("utils.write_to_bigquery")
def test_write_to_bigquery(mock_write, spark):
    # Create a dummy DataFrame to "write" to BigQuery
    data = [("user1", 120), ("user2", 130)]
    columns = ["user_id", "heart_rate"]
    test_df = spark.createDataFrame(data, columns)
    
    # Mock the write operation to avoid actual data upload
    mock_write.return_value = None  # Simulate successful write (no return value)
    
    # Call the write function (which will now use the mocked version)
    write_to_bigquery(test_df, "gold", "test_heart_rate")
    
    # Assert that the mock write function was called with the expected arguments
    mock_write.assert_called_once_with(test_df, "gold", "test_heart_rate")
