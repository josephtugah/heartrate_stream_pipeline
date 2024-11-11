import json
import logging
from pyspark.sql import DataFrame, SparkSession
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration from config.json
with open("config.json") as config_file:
    config = json.load(config_file)

# Initialize Spark session
def get_spark_session(app_name="Spark BigQuery App"):
    google_credentials = "/mnt/data/compute-engine-key.json"
    if not google_credentials:
        logging.error("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable must be set.")
    
    return SparkSession.builder \
        .appName(app_name) \
        .master('yarn') \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", google_credentials) \
        .getOrCreate()

# Clear any cached data in Spark
spark = get_spark_session()
spark.catalog.clearCache()

# Function to read data from BigQuery
def read_from_bigquery(spark, dataset_key, table_key):
    """Reads a table from BigQuery based on config.json keys."""
    try:
        project_id = config["bigquery"]["project_id"]
        dataset = config["bigquery"]["datasets"][dataset_key]["dataset"]
        table = config["bigquery"]["datasets"][dataset_key]["tables"][table_key]
        
        logging.info(f"Reading data from BigQuery table: {dataset}.{table} in project: {project_id}")

        return spark.read \
            .format("bigquery") \
            .option("project", project_id) \
            .option("dataset", dataset) \
            .option("table", table) \
            .load()
        
    except Exception as e:
        logging.error(f"Error reading data from BigQuery: {str(e)}")
        raise

# Function to write data to BigQuery
def write_to_bigquery(df: DataFrame, dataset_key: str, table_key: str, mode="overwrite"):
    """Writes a DataFrame to BigQuery based on config.json keys."""
    try:
        project_id = config["bigquery"]["project_id"]
        dataset = config["bigquery"]["datasets"][dataset_key]["dataset"]
        table = config["bigquery"]["datasets"][dataset_key]["tables"][table_key]
        gcs_temp_bucket = config["bigquery"].get("gcs_temp_bucket", "default-dataproc-temp-bucket")
        
        logging.info(f"Writing data to BigQuery table: {dataset}.{table} in project: {project_id}")

        df.write \
            .format("bigquery") \
            .option("project", project_id) \
            .option("dataset", dataset) \
            .option("table", table) \
            .option("temporaryGcsBucket", gcs_temp_bucket) \
            .mode(mode) \
            .save()

        logging.info(f"Data written successfully to {dataset}.{table}.")
    except Exception as e:
        logging.error(f"Error writing data to BigQuery: {str(e)}")
        raise
