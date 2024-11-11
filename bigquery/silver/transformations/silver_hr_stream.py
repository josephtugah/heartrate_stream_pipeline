from utils import get_spark_session, read_from_bigquery, write_to_bigquery
from pyspark.sql.functions import col, from_unixtime, from_json
from pyspark.sql.types import StructType, StructField, LongType, DoubleType

def transform_and_load_heartrate_data(spark):
    # Define the schema for the meta field (since it's a JSON field)
    meta_schema = StructType([
        StructField("activity_id", LongType(), True),
        StructField("location", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ]), True)
    ])

    # Read from Bronze layer
    bronze_heartrate_df = read_from_bigquery(spark, "bronze", "heartrate")

    # Parse the meta field from JSON string to struct
    bronze_heartrate_df = bronze_heartrate_df.withColumn(
        "meta", from_json(col("meta"), meta_schema)
    )

    # Register the DataFrame as a temporary view to use with Spark SQL
    bronze_heartrate_df.createOrReplaceTempView("bronze_heartrate")

    # SQL Transformation for Silver Layer
    dim_heartrate_sql = """
        SELECT
            user_id,
            heart_rate,
            from_unixtime(timestamp) AS event_timestamp,  -- Converting Unix timestamp to readable format
            meta.activity_id AS activity_id,
            meta.location.latitude AS latitude,
            meta.location.longitude AS longitude
        FROM
            bronze_heartrate
        WHERE
            user_id IS NOT NULL  -- Optional: filtering out records with null user IDs
    """

    # Execute the SQL transformation
    silver_heartrate_df = spark.sql(dim_heartrate_sql)

    # Show the schema of the resulting DataFrame to confirm proper field extraction
    silver_heartrate_df.printSchema()

    # Write to Silver layer in BigQuery
    write_to_bigquery(silver_heartrate_df, "silver", "heartrate", mode="overwrite")
    print("Heart rate data written to BigQuery successfully.")
