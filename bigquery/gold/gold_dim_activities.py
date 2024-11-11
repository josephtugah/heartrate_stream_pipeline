from utils import get_spark_session, read_from_bigquery, write_to_bigquery

def move_dim_activities_to_gold():
    # Initialize Spark session
    spark = get_spark_session("Move dim_activities to Gold Layer")

    # Load dim_activities from Silver layer
    dim_activities_df = read_from_bigquery(spark, "silver", "activities")

    # Write dim_activities to Gold layer
    write_to_bigquery(dim_activities_df, "gold", "dim_activities")
    print("dim_activities table successfully transferred to Gold layer.")
