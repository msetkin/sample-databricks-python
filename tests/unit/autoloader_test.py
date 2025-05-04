from pyspark.sql import SparkSession

from python_helper.autoloader import Autoloader


def test_autoloader_init_default_values(spark: SparkSession):
    input_path = "/tmp"
    select_columns = ["col1"]

    autoloader = Autoloader(spark=spark, input_path=input_path, select_columns=select_columns)

    assert autoloader.spark == spark
    assert autoloader.input_path == input_path
    assert autoloader.select_columns == select_columns
    assert autoloader.allow_overwrites is True
    assert autoloader.partition_columns is None
    assert autoloader.drop_columns is None


def test_autoloader_init_non_default_values_wo_part(spark: SparkSession):
    input_path = "/tmp"
    select_columns = ["col1"]
    allow_overwrites: bool = False
    drop_columns = ["col2"]

    autoloader = Autoloader(
        spark=spark,
        input_path=input_path,
        select_columns=select_columns,
        allow_overwrites=allow_overwrites,
        drop_columns=drop_columns,
    )

    assert autoloader.spark == spark
    assert autoloader.input_path == input_path
    assert autoloader.select_columns == select_columns
    assert autoloader.allow_overwrites is allow_overwrites
    assert autoloader.drop_columns == drop_columns
    assert autoloader.partition_columns is None


def test_autoloader_init_non_default_values_with_part(spark: SparkSession):
    input_path = "/tmp"
    select_columns = ["col1"]
    allow_overwrites: bool = False
    partition_columns = "col1"
    drop_columns = ["col2"]

    autoloader = Autoloader(
        spark=spark,
        input_path=input_path,
        select_columns=select_columns,
        partition_columns=partition_columns,
        allow_overwrites=allow_overwrites,
        drop_columns=drop_columns,
    )

    assert autoloader.spark == spark
    assert autoloader.input_path == input_path
    assert autoloader.select_columns == select_columns
    assert autoloader.allow_overwrites is allow_overwrites
    assert autoloader.partition_columns == partition_columns
    assert autoloader.drop_columns is drop_columns
