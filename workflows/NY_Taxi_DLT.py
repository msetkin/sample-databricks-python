# Databricks notebook source
# MAGIC %pip install "{spark.conf.get('python_helper_path')}"

# COMMAND ----------

from python_helper.common import JinjaVarsHelper
from python_helper.pipelines.ny_taxi import NY_Taxi

# COMMAND ----------

github_run_id = spark.conf.get("github_run_id")
env = spark.conf.get("env")
conf_file = spark.conf.get("conf_file")

# TODO: need to define a path as an argument
jinja_vars_helper = JinjaVarsHelper(conf_file, env, github_run_id)

ny_taxi_pipeline = NY_Taxi(jinja_vars_helper=jinja_vars_helper, github_run_id=github_run_id)

ny_taxi_pipeline.define_pipelines()
