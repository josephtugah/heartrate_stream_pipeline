from utils import get_spark_session, read_from_bigquery, write_to_bigquery
from pyspark.sql import functions as F
from pyspark.sql import Window

def transform_and_load_heart_rate_fact_table(spark):
    # Load necessary tables from the Silver layer
    users_df = read_from_bigquery(spark, "silver", "dim_users")
    activities_df = read_from_bigquery(spark, "silver", "dim_activities")
    dates_df = read_from_bigquery(spark, "silver", "date_dim")
    times_df = read_from_bigquery(spark, "silver", "time_dim")
    heart_rate_df = read_from_bigquery(spark, "silver", "heartrate_cleaned")

    # Create temporary views for SQL transformations
    users_df.createOrReplaceTempView("dim_users")
    activities_df.createOrReplaceTempView("dim_activities")
    dates_df.createOrReplaceTempView("dim_dates")
    times_df.createOrReplaceTempView("dim_times")
    heart_rate_df.createOrReplaceTempView("heart_rate_cleaned")

    # SQL Transformation for the heart rate fact table
    heart_rate_transformed_sql = """
        SELECT
            hr.user_id AS user_key,
            hr.heart_rate,
            TO_DATE(hr.event_timestamp) AS event_date,
            DATE_FORMAT(hr.event_timestamp, 'HH:mm:ss') AS event_time,
            LAG(hr.heart_rate) OVER (PARTITION BY hr.user_id ORDER BY hr.event_timestamp) AS previous_heart_rate,
            MAX(hr.heart_rate) OVER (PARTITION BY hr.user_id, hr.activity_id, TO_DATE(hr.event_timestamp)) AS max_heart_rate,
            MIN(hr.heart_rate) OVER (PARTITION BY hr.user_id, hr.activity_id, TO_DATE(hr.event_timestamp)) AS min_heart_rate,
            AVG(hr.heart_rate) OVER (PARTITION BY hr.user_id, hr.activity_id, TO_DATE(hr.event_timestamp)) AS avg_heart_rate,
            hr.activity_id,
            hr.latitude,
            hr.longitude
        FROM
            heart_rate_cleaned hr
        LEFT JOIN
            dim_users u ON hr.user_id = u.user_id
        LEFT JOIN
            dim_activities a ON hr.activity_id = a.activity_id
        LEFT JOIN
            dim_dates d ON TO_DATE(hr.event_timestamp) = d.date_day
        LEFT JOIN
            dim_times t ON DATE_FORMAT(hr.event_timestamp, 'HH:mm:ss') = t.hh_mm_ss
    """

    # Execute the transformation query
    heart_rate_transformed_df = spark.sql(heart_rate_transformed_sql)

    # Write the result to the Gold layer in BigQuery
    write_to_bigquery(heart_rate_transformed_df, "gold", "fct_heart_rate")
    print("Heart rate fact table transformation and load completed successfully.")
