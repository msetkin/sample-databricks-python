# Databricks notebook source
# MAGIC %pip install "{dbutils.widgets.ge('python_helper_path')}"

# COMMAND ----------
import os
import shutil

from python_helper.common import DBUtilities, JinjaVarsHelper, get_spark_session
from tests.shared_test_instruments.ny_taxi_data import (
    ny_taxi_bronze_sample_ride,
    pep_pickup_date_txt,
)
from tests.shared_test_instruments.utils import save_dict_to_random_path

# COMMAND ----------
spark = get_spark_session()
db_utils = DBUtilities.get_dbutils(spark)
github_run_id = db_utils.widgets.get("github_run_id")

# COMMAND ----------
jinja_vars_helper = JinjaVarsHelper(
    db_utils.widgets.get("conf_file"),
    db_utils.widgets.get("env"),
    github_run_id
)
ny_taxi_src_path = jinja_vars_helper.get_ny_taxi_src_path()

# COMMAND ----------
# clean-up from previous runs:
db_utils.fs.rm(ny_taxi_src_path, True)

# COMMAND ----------

ny_taxi_path_w_part = f"{ny_taxi_src_path}/pep_pickup_date_txt={pep_pickup_date_txt}/"


# COMMAND ----------
# upload the file to S3
ny_taxi_data = str(ny_taxi_bronze_sample_ride.dict())
ny_taxi_local_path = save_dict_to_random_path(ny_taxi_data)

db_utils.fs.cp(ny_taxi_local_path, ny_taxi_path_w_part)

directory_to_remove = os.path.dirname(ny_taxi_local_path)
shutil.rmtree(directory_to_remove)
