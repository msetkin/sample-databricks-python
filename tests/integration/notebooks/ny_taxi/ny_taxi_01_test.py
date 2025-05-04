# Databricks notebook source
# MAGIC %pip install "{dbutils.widgets.ge('python_helper_path')}"

# COMMAND ----------
from chispa.dataframe_comparer import assert_df_equality

from python_helper.common import (
    DBUtilities,
    JinjaVarsHelper,
    dataframe_except_columns,
    get_spark_session,
)
from tests.shared_test_instruments.ny_taxi_data import (
    ny_taxi_silver_sample_ride,
    ny_taxi_silver_schema,
)

# COMMAND ----------

spark = get_spark_session()
db_utils = DBUtilities.get_dbutils(spark)

conf_file = db_utils.widgets.get("conf_file")
env = db_utils.widgets.get("env")
github_run_id = db_utils.widgets.get("github_run_id")

# COMMAND ----------

jinja_vars_helper = JinjaVarsHelper(
    conf_file,
    env,
    github_run_id,
)

# COMMAND ----------
# get the two "real" silver tables into dataframes
db_schema = jinja_vars_helper.get_db_data_schema()
silver_ny_taxi_df = spark.table(f"{db_schema}.silver_ny_taxi")
silver_ny_taxi_scd1_df = spark.table(f"{db_schema}.silver_ny_taxi_scd1")    

# COMMAND ----------
# generate the expected result
ny_taxi_silver_sample_ride_dict = ny_taxi_silver_sample_ride.dict(by_alias=True)
expected_silver_ny_taxi_df = spark.createDataFrame([ny_taxi_silver_sample_ride_dict], ny_taxi_silver_schema)
expected_silver_ny_taxi_scd1_df = spark.createDataFrame([ny_taxi_silver_sample_ride_dict], ny_taxi_silver_schema)

# COMMAND ----------
# compare the result
assert_df_equality(
    dataframe_except_columns(silver_ny_taxi_df, ["_processing_date", "_processing_ts"]),
    expected_silver_ny_taxi_df,
    ignore_nullable=True,
)

assert_df_equality(
    dataframe_except_columns(silver_ny_taxi_df, ["_processing_date", "_processing_ts"]),
    expected_silver_ny_taxi_scd1_df,
    ignore_nullable=True,
)
