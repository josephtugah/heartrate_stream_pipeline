import pytest
from utils import read_from_bigquery
from pyspark.sql import functions as F

@pytest.fixture(scope="module")
def silver_activities_df(spark):
    # Simulate the Silver activities transformation output
    return read_from_bigquery(spark, "silver", "activities")

def test_activity_schema(silver_activities_df):
    # Check that the schema matches expected columns
    expected_columns = ["activity_id", "activity_name", "start_date", "end_date"]
    actual_columns = silver_activities_df.columns
    assert set(expected_columns) == set(actual_columns), f"Expected columns: {expected_columns}, but got {actual_columns}"

def test_activity_data(silver_activities_df):
    # Test that activity_id is not null
    assert silver_activities_df.filter(F.col("activity_id").isNull()).count() == 0, "Found null activity_id"
    
    # Test that start_date is of type DATE
    assert silver_activities_df.filter(F.col("start_date").cast("date").isNotNull()).count() == silver_activities_df.count(), "start_date contains invalid dates"
    
    # Test that end_date is NULL
    assert silver_activities_df.filter(F.col("end_date").isNotNull()).count() == 0, "Found non-null end_date"

