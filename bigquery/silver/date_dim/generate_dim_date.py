from utils import get_spark_session, write_to_bigquery
from pyspark.sql import functions as F
import datetime

# Function to generate date dimension
def generate_date_dim(spark):
    # Define date range
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2026, 12, 31)

    # Generate a sequence of dates
    date_dim_df = spark.createDataFrame(
        [(start_date + datetime.timedelta(days=i),) for i in range((end_date - start_date).days + 1)],
        ["date"]
    )

    # Register as a temporary view for SQL processing
    date_dim_df.createOrReplaceTempView("date_sequence")

    # Spark SQL transformation for date dimension with additional fields
    dim_dates_sql = """
        SELECT
            CAST(date as DATE) AS date_day,  -- Primary key in YYYY-MM-DD format
            date,
            DATE_SUB(date, 1) AS prior_date_day,
            DATE_ADD(date, 1) AS next_date_day,
            DATE_SUB(date, 365) AS prior_year_date_day,  -- Assumes no leap-year adjustment
            DATE_SUB(date, 364) AS prior_year_over_year_date_day,  -- Adjusted for leap-year continuity

            -- Year start and end dates
            DATE_TRUNC(date, 'YEAR') AS year_start_date,
            LAST_DAY(date) AS year_end_date,  -- Correct usage of LAST_DAY function

            -- Additional standard date dimensions
            DAY(date) AS day,
            CASE WHEN DATE_FORMAT(date, 'E') = 'Sun' THEN 7
                 WHEN DATE_FORMAT(date, 'E') = 'Mon' THEN 1
                 WHEN DATE_FORMAT(date, 'E') = 'Tue' THEN 2
                 WHEN DATE_FORMAT(date, 'E') = 'Wed' THEN 3
                 WHEN DATE_FORMAT(date, 'E') = 'Thu' THEN 4
                 WHEN DATE_FORMAT(date, 'E') = 'Fri' THEN 5
                 WHEN DATE_FORMAT(date, 'E') = 'Sat' THEN 6
                 ELSE NULL END AS day_of_week,
            DATE_FORMAT(date, 'EEEE') AS day_name,
            CASE WHEN DATE_FORMAT(date, 'E') IN ('Sat', 'Sun') THEN true ELSE false END AS is_weekend
        FROM
            date_sequence
    """

    # Execute the SQL transformation
    dim_dates_df = spark.sql(dim_dates_sql)

    # Write to BigQuery
    write_to_bigquery(dim_dates_df, "silver", "dim_dates", mode="overwrite")
    print("Date dimension data written to BigQuery successfully.")
