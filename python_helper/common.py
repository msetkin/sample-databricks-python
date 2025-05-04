import logging
import pathlib
from typing import Dict, List

import pyspark.sql.functions as F
import yaml
from pyspark.sql import Column, DataFrame, SparkSession

ENV_LIST = ["stage", "prod"]


# https://github.com/alexott/dlt-files-in-repos-demo/blob/main/tests/unit-local/test_columns_helpers.py
def columns_except(df: DataFrame, ex: List[str] | None = None, as_column: bool = False) -> List[str | Column]:
    """
    Returns a new list of columns without specified columns
    :param df: dataframe
    :param ex: columns to exclude
    :param as_column: if we should return list of columns instead of list of strings
    :return: new list of columns
    """
    if ex is None:
        ex = []

    return [F.col(cl) if as_column else cl for cl in df.columns if cl.upper() not in [x.upper() for x in ex]]


def column_exists(data_frame: DataFrame, column_name: str) -> bool:
    return column_name.upper() in [x.upper() for x in data_frame.columns]


def dataframe_except_columns(df: DataFrame, ex: List[str] | None = None) -> DataFrame:
    """
    Creates a new dataframe without specified columns
    :param df: dataframe
    :param ex: columns to exclude
    :return: new dataframe
    """
    if ex is not None:
        for field in ex:
            if not column_exists(df, field):
                raise RuntimeError(f"The column {field} doesn't exist in the dataframe {df}")
    return df.select(*columns_except(df, ex, as_column=True))


def get_spark_session(spark: SparkSession | None) -> SparkSession:
    if not spark:
        return SparkSession.builder.getOrCreate()
    else:
        return spark


def none_to_empty_str(input_str: str | None) -> str:
    return input_str or ""


def get_table_postfix(github_run_id: str) -> str:
    return "_" + github_run_id if none_to_empty_str(github_run_id) else ""


def get_physical_table_name(logical_table_name: str, github_run_id: str) -> str:
    return f"{logical_table_name}{get_table_postfix(github_run_id)}"


def validate_incoming_env_value(env: str) -> None:
    if env not in ENV_LIST:
        raise KeyError(f"Value: '{env}' does not exist in a environments list: {ENV_LIST}")
    else:
        pass


class JinjaVarsHelper:
    __conf_file: str
    __env: str
    __github_run_id: str
    __env_config: Dict

    def __init__(self, conf_file: str, env: str, github_run_id: str | None = ""):
        validate_incoming_env_value(env)
        self.__conf_file = conf_file
        self.__env = env
        self.__github_run_id = none_to_empty_str(github_run_id)
        config: Dict = yaml.safe_load(pathlib.Path(self.__conf_file).absolute().read_text())
        self.__env_config = config["environment"][self.__env]

    def get_db_data_schema(self) -> str:
        return self.__env_config["db_data_schema"]

    def get_s3_bucket(self) -> str:
        return self.__env_config["s3_bucket"]

    def get_ny_taxi_src_path(self) -> str:
        if self.__env == "prod":
            return "/databricks-datasets/nyctaxi/sample/json/"
        else:
            if not self.__github_run_id:
                raise RuntimeError(f"empty github_run_id is not allowed for the env: {self.__env}")
            return f"s3://{self.get_s3_bucket()}/data/input/{self.__github_run_id}/uber/ny_taxi/"


class DBUtilities:
    @staticmethod
    def get_dbutils(spark: SparkSession):  # type: ignore
        try:
            from pyspark.dbutils import DBUtils  # noqa # type: ignore

            if "dbutils" not in locals():
                return DBUtils(spark)
            else:
                return locals().get("dbutils")
        except ModuleNotFoundError:
            logging.warning(
                "DBUtils import failure. Importing a mock-up library. This is OK if inside unit-tests. "
                "Otherwise check the code."
            )
            from databricks_test import DbUtils

            return DbUtils()
