import pytest
from utils import read_from_bigquery
from pyspark.sql import functions as F

@pytest.fixture(scope="module")
def dim_date_df(spark):
    # Simulate the Date Dimension transformation output
    return read_from_bigquery(spark, "silver", "date_dim")

def test_date_dimension(dim_date_df):
    # Check that the date dimension includes all expected fields
    expected_columns = ["date_day", "date", "prior_date_day", "next_date_day", "prior_year_date_day", "prior_year_over_year_date_day", "year_start_date", "year_end_date", "day", "day_of_week", "day_name", "is_weekend"]
    actual_columns = dim_date_df.columns
    assert set(expected_columns) == set(actual_columns), f"Expected columns: {expected_columns}, but got {actual_columns}"
    
    # Ensure that no null dates exist
    assert dim_date_df.filter(F.col("date_day").isNull()).count() == 0, "Found null date_day"
