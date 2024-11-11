import pytest
from utils import read_from_bigquery
from pyspark.sql import functions as F

@pytest.fixture(scope="module")
def silver_users_df(spark):
    # Simulate the Silver users transformation output
    return read_from_bigquery(spark, "silver", "users")

def test_user_schema(silver_users_df):
    # Check that the schema matches expected columns
    expected_columns = ["user_id", "first_name", "last_name", "date_of_birth", "gender", "height", "weight", "blood_type", "race", "origin_country", "address", "address_country", "start_date", "end_date"]
    actual_columns = silver_users_df.columns
    assert set(expected_columns) == set(actual_columns), f"Expected columns: {expected_columns}, but got {actual_columns}"

def test_user_data(silver_users_df):
    # Test that user_id is not null
    assert silver_users_df.filter(F.col("user_id").isNull()).count() == 0, "Found null user_id"
    
    # Test that date_of_birth is in the correct date format
    assert silver_users_df.filter(F.col("date_of_birth").cast("date").isNotNull()).count() == silver_users_df.count(), "Invalid date_of_birth format"
    
    # Test that start_date is of type DATE
    assert silver_users_df.filter(F.col("start_date").cast("date").isNotNull()).count() == silver_users_df.count(), "Invalid start_date format"
