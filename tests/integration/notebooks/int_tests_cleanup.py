# Databricks notebook source
# MAGIC %pip install "{dbutils.widgets.ge('python_helper_path')}"

# COMMAND ----------

from python_helper.common import DBUtilities, JinjaVarsHelper, get_spark_session
from tests.shared_test_instruments.utils import clean_up_environment_from_tables

# COMMAND ----------
spark = get_spark_session()
db_utils = DBUtilities.get_dbutils(spark)

# COMMAND ----------
github_run_id = db_utils.widgets.get("github_run_id")

conf_file = db_utils.widgets.get("conf_file")
env = db_utils.widgets.get("env")

jinja_vars_helper = JinjaVarsHelper(
    conf_file,
    env,
    github_run_id
)

# COMMAND ----------

db_schema = jinja_vars_helper.get_db_data_schema()
my_taxi_input_location = jinja_vars_helper.get_ny_taxi_src_path()

# COMMAND ----------

db_utils = DBUtilities.get_dbutils(spark)

# COMMAND ----------
# dropping the test input files from S3
db_utils.fs.rm(my_taxi_input_location, True)

# COMMAND ----------
# dropping the tables and views from Databricks:

# COMMAND ----------
clean_up_environment_from_tables()
