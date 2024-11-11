from utils import get_spark_session, read_from_bigquery, write_to_bigquery

def transform_and_load_activities_data(spark):
    # Read from Bronze layer
    bronze_activities_df = read_from_bigquery(spark, "bronze", "activities")

    # Register the DataFrame as a temporary view to use with Spark SQL
    bronze_activities_df.createOrReplaceTempView("bronze_activities")

    # SQL Transformation for Silver Layer
    dim_activities_sql = """
        SELECT
            activity_id,
            activity_name AS activity_name,
            CAST(last_update AS DATE) AS start_date,
            NULL AS end_date  -- Setting end_date to NULL for active records
        FROM
            bronze_activities
        WHERE
            activity_id IS NOT NULL  -- Optional: filtering out records with null activity IDs
    """

    # Execute the SQL transformation
    silver_activities_df = spark.sql(dim_activities_sql)

    # Write to Silver layer in BigQuery
    write_to_bigquery(silver_activities_df, "silver", "activities", mode="overwrite")
    print("Activities data written to BigQuery successfully.")