import logging
import os
import shutil
import tempfile
import typing
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="session")
def spark() -> typing.Generator[SparkSession, None, None]:
    """
    This fixture provides preconfigured SparkSession with Hive and Delta support.
    After the test session, temporary warehouse directory is deleted.
    :return: SparkSession
    """
    local_maven_repo_var = "LOCAL_MAVEN_REPO"
    logging.info("Configuring Spark session for testing environment")
    warehouse_dir = tempfile.TemporaryDirectory().name
    _builder = (
        SparkSession.builder.master("local[1]")
        .config("spark.hive.metastore.warehouse.dir", Path(warehouse_dir).as_uri())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.allowArbitraryProperties.enabled", "true")
    )
    # Check if the environment variable is set and configure the repository if it is
    if local_maven_repo_var in os.environ:
        local_repo_url = os.environ[local_maven_repo_var]
        if not local_repo_url:
            raise RuntimeError(f"the environment variable '{local_maven_repo_var}' is set, but it's value is empty")
        logging.info(
            f"Environment variable '{local_maven_repo_var}' is set to '{local_repo_url}'. "
            "Configuring spark.jars.repositories."
        )
        _builder.config("spark.jars.repositories", os.environ[local_maven_repo_var])
    else:
        logging.info(f"Environment variable '{local_maven_repo_var}' is not set. Using default repository.")

    spark: SparkSession = configure_spark_with_delta_pip(_builder).getOrCreate()
    logging.info("Spark session configured")
    yield spark
    logging.info("Shutting down Spark session")
    spark.stop()
    if Path(warehouse_dir).exists():
        shutil.rmtree(warehouse_dir)


@pytest.fixture(scope="session")
def sample_df(spark: SparkSession) -> DataFrame:
    data = [
        {"x": "1", "y": "a"},
        {"x": "2", "y": "b"},
    ]
    return spark.createDataFrame(data)
