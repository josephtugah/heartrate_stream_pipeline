import pytest
from utils import read_from_bigquery
from pyspark.sql import functions as F

@pytest.fixture(scope="module")
def silver_heartrate_df(spark):
    # Simulate the Silver heartrate transformation output
    return read_from_bigquery(spark, "silver", "heartrate")

def test_heartrate_schema(silver_heartrate_df):
    # Check that the schema matches expected columns
    expected_columns = ["user_id", "heart_rate", "event_timestamp", "activity_id", "latitude", "longitude"]
    actual_columns = silver_heartrate_df.columns
    assert set(expected_columns) == set(actual_columns), f"Expected columns: {expected_columns}, but got {actual_columns}"

def test_heartrate_data(silver_heartrate_df):
    # Test that user_id and heart_rate are not null
    assert silver_heartrate_df.filter(F.col("user_id").isNull()).count() == 0, "Found null user_id"
    assert silver_heartrate_df.filter(F.col("heart_rate").isNull()).count() == 0, "Found null heart_rate"
    
    # Test that event_timestamp is a valid timestamp
    assert silver_heartrate_df.filter(F.col("event_timestamp").cast("timestamp").isNotNull()).count() == silver_heartrate_df.count(), "Invalid event_timestamp format"
