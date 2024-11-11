import pytest
from utils import read_from_bigquery
from pyspark.sql import functions as F

@pytest.fixture(scope="module")
def fct_heart_rate_df(spark):
    # Simulate the Gold fact table transformation output
    return read_from_bigquery(spark, "gold", "fct_heart_rate")

def test_fact_table_schema(fct_heart_rate_df):
    # Check that the schema matches expected columns
    expected_columns = ["user_key", "heart_rate", "event_date", "event_time", 
                        "previous_heart_rate", "max_heart_rate", "min_heart_rate", 
                        "avg_heart_rate", "activity_id", "latitude", "longitude"]
    actual_columns = fct_heart_rate_df.columns
    assert set(expected_columns) == set(actual_columns), f"Expected columns: {expected_columns}, but got {actual_columns}"

def test_fact_table_data(fct_heart_rate_df):
    # Ensure that no null values exist for heart_rate or event_date
    assert fct_heart_rate_df.filter(F.col("heart_rate").isNull()).count() == 0, "Found null heart_rate"
    assert fct_heart_rate_df.filter(F.col("event_date").isNull()).count() == 0, "Found null event_date"
    
    # Check if there are any null values in columns that should not have null values
    non_nullable_columns = ["user_key", "event_time", "previous_heart_rate", "max_heart_rate", "min_heart_rate", "avg_heart_rate"]
    for column in non_nullable_columns:
        assert fct_heart_rate_df.filter(F.col(column).isNull()).count() == 0, f"Found null in {column}"

def test_heart_rate_aggregations(fct_heart_rate_df):
    # Check that max_heart_rate, min_heart_rate, and avg_heart_rate are correctly calculated
    # For this test, we will group by user_key, activity_id, and event_date to calculate expected max, min, and avg

    # Aggregate heart rate data by user, activity, and date
    aggregated_data = fct_heart_rate_df.groupBy("user_key", "activity_id", "event_date").agg(
        F.max("heart_rate").alias("max_heart_rate"),
        F.min("heart_rate").alias("min_heart_rate"),
        F.avg("heart_rate").alias("avg_heart_rate")
    )

    # Compare calculated aggregations with those in the fact table
    for row in aggregated_data.collect():
        user_key, activity_id, event_date = row["user_key"], row["activity_id"], row["event_date"]
        max_hr = row["max_heart_rate"]
        min_hr = row["min_heart_rate"]
        avg_hr = row["avg_heart_rate"]

        # Filter the fact table data for the current user, activity, and date
        fact_data = fct_heart_rate_df.filter(
            (F.col("user_key") == user_key) & 
            (F.col("activity_id") == activity_id) & 
            (F.col("event_date") == event_date)
        )

        # Check that the max, min, and avg heart rate in the fact table matches the aggregated values
        for fact_row in fact_data.collect():
            assert fact_row["max_heart_rate"] == max_hr, f"Max heart rate mismatch for {user_key}, {activity_id}, {event_date}"
            assert fact_row["min_heart_rate"] == min_hr, f"Min heart rate mismatch for {user_key}, {activity_id}, {event_date}"
            assert fact_row["avg_heart_rate"] == avg_hr, f"Avg heart rate mismatch for {user_key}, {activity_id}, {event_date}"

def test_previous_heart_rate(fct_heart_rate_df):
    # Validate that the 'previous_heart_rate' column is calculated correctly using LAG
    user_data = fct_heart_rate_df.filter(F.col("user_key") == 1).orderBy("event_date", "event_time").collect()
    
    # Check that the 'previous_heart_rate' for each record is the heart rate of the previous event for the same user
    for i in range(1, len(user_data)):
        assert user_data[i]['previous_heart_rate'] == user_data[i-1]['heart_rate'], \
            f"Previous heart rate mismatch for user {user_data[i]['user_key']} at {user_data[i]['event_date']}"

def test_no_null_previous_heart_rate(fct_heart_rate_df):
    # Ensure that the first record for each user has no previous heart rate (it should be null)
    first_records = fct_heart_rate_df.groupBy("user_key").agg(F.min("event_date").alias("first_date"))
    for row in first_records.collect():
        user_key = row["user_key"]
        first_date = row["first_date"]
        
        # Find the first record for each user and check if previous_heart_rate is null
        first_record = fct_heart_rate_df.filter(
            (F.col("user_key") == user_key) & 
            (F.col("event_date") == first_date)
        ).first()
        
        assert first_record["previous_heart_rate"] is None, \
            f"Previous heart rate should be None for the first record of user {user_key} on {first_date}"

def test_no_null_heart_rate_or_event_date(fct_heart_rate_df):
    # Check for no null values in the heart_rate and event_date columns
    assert fct_heart_rate_df.filter(F.col("heart_rate").isNull()).count() == 0, "Null heart_rate found"
    assert fct_heart_rate_df.filter(F.col("event_date").isNull()).count() == 0, "Null event_date found"
