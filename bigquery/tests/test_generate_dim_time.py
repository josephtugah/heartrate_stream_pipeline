import pytest
from utils import read_from_bigquery
from pyspark.sql import functions as F

@pytest.fixture(scope="module")
def dim_time_df(spark):
    # Simulate the Time Dimension transformation output
    return read_from_bigquery(spark, "silver", "time_dim")

def test_time_dimension(dim_time_df):
    # Check that the time dimension includes all expected fields
    expected_columns = ["time_of_day", "hour", "minute", "second", "time_of_day"]
    actual_columns = dim_time_df.columns
    assert set(expected_columns) == set(actual_columns), f"Expected columns: {expected_columns}, but got {actual_columns}"

    # Ensure that time_of_day is correctly categorized
    categories = ['Early Morning', 'Morning', 'Afternoon', 'Evening', 'Night']
    for category in categories:
        assert dim_time_df.filter(F.col("time_of_day") == category).count() > 0, f"No data for category {category}"
