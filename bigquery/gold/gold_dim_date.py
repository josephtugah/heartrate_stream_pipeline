from utils import get_spark_session, read_from_bigquery, write_to_bigquery

def move_dim_dates_to_gold():
    # Initialize Spark session
    spark = get_spark_session("Date Dimension Transfer to Gold")

    # Read dim_dates from Silver layer
    dim_dates_df = read_from_bigquery(spark, "silver", "date_dim")

    # Write dim_dates to Gold layer without transformations
    write_to_bigquery(dim_dates_df, "gold", "dim_date")
    print("dim_dates table successfully transferred to Gold layer.")
