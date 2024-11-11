import pandas as pd
from utils import get_spark_session, read_from_bigquery, write_to_bigquery

def transform_and_load_users_data(spark):
    # Read from Bronze layer
    bronze_users_df = read_from_bigquery(spark, "bronze", "users")

    # Register the DataFrame as a temporary view to use with Spark SQL
    bronze_users_df.createOrReplaceTempView("bronze_users")

    # Perform transformations using Spark SQL
    dim_users_sql = """
    SELECT
        userid AS user_id,
        fname AS first_name,
        lname AS last_name,
        CAST(CONCAT(SUBSTRING(CAST(dob AS STRING), 1, 4), '-', 
                    SUBSTRING(CAST(dob AS STRING), 5, 2), '-', 
                    SUBSTRING(CAST(dob AS STRING), 7, 2)) AS DATE) AS date_of_birth,  -- Convert dob from BIGINT to DATE
        sex AS gender,
        height,
        weight,
        blood_type,
        race,
        origin_country_name AS origin_country,
        address,
        address_country_name AS address_country,
        CAST(last_update AS DATE) AS start_date,
        NULL AS end_date  -- Assuming end_date is used to track inactive records in the future
    FROM
        bronze_users
    WHERE
        userid IS NOT NULL  -- Optional: filtering out records with null user IDs
"""


    # Execute the SQL transformation
    silver_users_df = spark.sql(dim_users_sql)

    # Write to BigQuery
    write_to_bigquery(silver_users_df, "silver", "users", mode="overwrite")
    print("Users data written to BigQuery successfully.")
