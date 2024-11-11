import os
import json
import logging
import pytest
from google.cloud import bigquery
from utils import get_spark_session, read_from_bigquery, write_to_bigquery
from silver.date_dim.generate_dim_date import generate_date_dim
from silver.date_dim.generate_dim_time import generate_time_dim
from gold.gold_fct_hr_stream import transform_and_load_heart_rate_fact_table
from silver.transformations.silver_users import transform_and_load_users_data
from silver.transformations.silver_activities import transform_and_load_activities_data
from silver.transformations.silver_hr_stream import transform_and_load_heartrate_data
from gold.gold_dim_users import move_dim_users_to_gold_layer
from gold.gold_dim_time import move_dim_times_to_gold_layer
from gold.gold_dim_date import move_dim_dates_to_gold
from gold.gold_dim_activities import move_dim_activities_to_gold

logging.basicConfig(level=logging.INFO)

def load_config():
    """Load configurations from config.json or environment variables."""
    config_path = os.getenv("CONFIG_PATH", "config.json")
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error("Configuration file not found at %s", config_path)
        raise

def get_bigquery_client():
    """Initialize BigQuery Client."""
    try:
        return bigquery.Client()
    except Exception as e:
        logging.error("Error initializing BigQuery client: %s", e)
        raise

def upload_to_bigquery(df, table_name, project_id, dataset_id):
    """Upload DataFrame to BigQuery."""
    try:
        df.to_gbq(destination_table=f"{dataset_id}.{table_name}", project_id=project_id, if_exists='replace')
        logging.info("Data uploaded to BigQuery table %s.%s", dataset_id, table_name)
    except Exception as e:
        logging.error("Error uploading to BigQuery table %s.%s: %s", dataset_id, table_name, e)
        raise

def run_transformations(spark):
    """Run transformations, load data, and execute tests for ETL pipeline."""
    try:
        logging.info("Running transformations...")

        # Transform and load data for each layer
        transformations = [
            ("Users Data", transform_and_load_users_data),
            ("Activities Data", transform_and_load_activities_data),
            ("Heart Rate Data", transform_and_load_heartrate_data),
            ("Date Dimension", generate_date_dim),
            ("Time Dimension", generate_time_dim)
        ]

        for name, func in transformations:
            logging.info("Transforming and Loading %s...", name)
            func(spark)

        # Run Silver Layer Tests
        logging.info("Running Silver Layer Tests...")
        silver_tests = [
            "bigquery/tests/test_utils.py",
            "bigquery/tests/test_silver_hr_stream.py",
            "bigquery/tests/test_silver_users.py",
            "bigquery/tests/test_silver_activities.py",
            "bigquery/tests/test_generate_dim_time.py",
            "bigquery/tests/test_generate_dim_date.py"
        ]
        for test in silver_tests:
            logging.info("Running %s...", test)
            pytest.main([test])

        # Transform and load data for the Gold layer
        logging.info("Transforming and Loading Heart Rate Fact Table...")
        transform_and_load_heart_rate_fact_table(spark)

        # Move dimensions to Gold layer
        gold_transformations = [
            ("dim_users", move_dim_users_to_gold_layer),
            ("dim_times", move_dim_times_to_gold_layer),
            ("dim_dates", move_dim_dates_to_gold),
            ("dim_activities", move_dim_activities_to_gold)
        ]

        for name, func in gold_transformations:
            logging.info("Moving %s to Gold Layer...", name)
            func(spark)

        # Run Gold Layer Tests
        logging.info("Running Gold Layer Tests...")
        gold_tests = ["bigquery/tests/test_gold_fct_hr_stream.py"]
        for test in gold_tests:
            logging.info("Running %s...", test)
            pytest.main([test])

        logging.info("All transformations and tests completed successfully.")

    except Exception as e:
        logging.error("Error during transformation and loading process: %s", e)
        raise

def main():
    """Main function to load config, initialize Spark session, and run ETL process."""
    # Load configuration
    config = load_config()

    # Get Spark session
    spark = get_spark_session()

    # Run ETL process
    run_transformations(spark)

if __name__ == "__main__":
    main()
