from utils import get_spark_session, read_from_bigquery, write_to_bigquery

def move_dim_times_to_gold_layer():
    # Initialize Spark session
    spark = get_spark_session("Move dim_times to Gold Layer")

    # Load dim_times from Silver layer
    dim_times_df = read_from_bigquery(spark, "silver", "time_dim")

    # Write dim_times to Gold layer
    write_to_bigquery(dim_times_df, "gold", "dim_time")
    print("dim_times table successfully moved to Gold layer.")
