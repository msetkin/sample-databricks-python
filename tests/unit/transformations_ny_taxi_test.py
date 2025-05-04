from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession

from python_helper.common import dataframe_except_columns
from python_helper.transformations.ny_taxi import (
    get_silver_ny_taxi_df,
)
from tests.shared_test_instruments.ny_taxi_data import (
    get_ny_taxi_bronze_sample_msg_w_tech_cols,
    get_ny_taxi_bronze_schema_w_tech_cols,
    ny_taxi_silver_sample_ride,
    ny_taxi_silver_schema,
)


def test_get_silver_ny_taxi_df(spark: SparkSession):
    bronze_input_message = spark.createDataFrame(
        [get_ny_taxi_bronze_sample_msg_w_tech_cols()], get_ny_taxi_bronze_schema_w_tech_cols()
    )

    fact_silver_df = get_silver_ny_taxi_df(bronze_input_message)

    ny_taxi_silver_sample_ride_dict = ny_taxi_silver_sample_ride.dict(by_alias=True)
    expected_silver_df = spark.createDataFrame([ny_taxi_silver_sample_ride_dict], ny_taxi_silver_schema)
    assert_df_equality(
        dataframe_except_columns(fact_silver_df, ["_processing_date", "_processing_ts"]),
        expected_silver_df,
        ignore_nullable=True,
    )
