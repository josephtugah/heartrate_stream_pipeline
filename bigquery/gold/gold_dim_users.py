from utils import get_spark_session, read_from_bigquery, write_to_bigquery

def move_dim_users_to_gold_layer():
    # Initialize Spark session
    spark = get_spark_session("Move dim_users to Gold Layer")

    # Load dim_users from Silver layer
    dim_users_df = read_from_bigquery(spark, "silver", "users")

    # Write dim_users to Gold layer
    write_to_bigquery(dim_users_df, "gold", "dim_users")
    print("dim_users table successfully moved to Gold layer.")
