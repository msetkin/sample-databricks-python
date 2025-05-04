from pyspark.sql import SparkSession

from python_helper.common import JinjaVarsHelper
from python_helper.pipelines.ny_taxi import NY_Taxi


def test_ny_taxi(spark: SparkSession):
    jinja_vars_helper = JinjaVarsHelper("tests/unit/data/jinja-vars.yml", "stage", "1")
    ny_taxi = NY_Taxi(jinja_vars_helper=jinja_vars_helper)
    ny_taxi.define_pipelines()
