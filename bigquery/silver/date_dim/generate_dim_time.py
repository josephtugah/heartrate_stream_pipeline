from utils import get_spark_session, write_to_bigquery
from pyspark.sql import functions as F
import datetime

def generate_time_dim(spark):
    # Define time range
    start_time = datetime.time(0, 0)  # 00:00:00
    end_time = datetime.time(23, 59)   # 23:59:00

    # Generate a sequence of times (for each minute)
    time_dim_df = spark.createDataFrame(
        [(datetime.datetime.combine(datetime.date.today(), start_time) + datetime.timedelta(minutes=i),) 
         for i in range((end_time.hour * 60 + end_time.minute) + 1)],
        ["time"]
    )

    # Register as a temporary view for SQL processing
    time_dim_df.createOrReplaceTempView("time_sequence")

    # Spark SQL transformation for time dimension with additional fields
    dim_times_sql = """
        SELECT
            DATE_FORMAT(time, 'HH:mm:ss') AS time_of_day,   -- Primary key
            HOUR(time) AS hour,                            -- Hour of the day (0-23)
            MINUTE(time) AS minute,                        -- Minute of the hour (0-59)
            SECOND(time) AS second,                        -- Second of the minute (0-59)
            
            -- Additional time dimension
            CASE 
                WHEN HOUR(time) BETWEEN 0 AND 5 THEN 'Early Morning'
                WHEN HOUR(time) BETWEEN 6 AND 11 THEN 'Morning'
                WHEN HOUR(time) BETWEEN 12 AND 17 THEN 'Afternoon'
                WHEN HOUR(time) BETWEEN 18 AND 21 THEN 'Evening'
                ELSE 'Night'
            END AS time_category  -- Categorization of the time of day
        FROM
            time_sequence
    """

    # Execute the SQL transformation
    dim_times_df = spark.sql(dim_times_sql)

    # Write to BigQuery
    write_to_bigquery(dim_times_df, "silver", "time_dim", mode="overwrite")
    print("Time dimension data written to BigQuery successfully.")
