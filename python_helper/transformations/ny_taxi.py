from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, date_format, when


def get_silver_ny_taxi_df(bronze_my_taxi_df: DataFrame) -> DataFrame:
    return bronze_my_taxi_df.select(
        "pep_pickup_date_txt",
        "DOLocationID",
        "PULocationID",
        "RatecodeID",
        "VendorID",
        "congestion_surcharge",
        "extra",
        "fare_amount",
        "improvement_surcharge",
        "mta_tax",
        "passenger_count",
        "payment_type",
        "store_and_fwd_flag",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "tpep_dropoff_datetime",
        "tpep_pickup_datetime",
        "trip_distance",
        when(col("trip_distance") < 1, True).otherwise(False).alias("is_short_ride"),
        col("_processing_ts").alias("_row_processing_ts"),
        date_format(current_timestamp(), "yyyy-MM-dd").alias("_processing_date"),
        current_timestamp().alias("_processing_ts"),
    )
