import mock
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession

from python_helper import common


def test_DBUtilities_get_dbutils_missing_library(spark: SparkSession, caplog):
    with mock.patch.dict("sys.modules", {"pyspark.dbutils": None}):
        dbutils = common.DBUtilities.get_dbutils(spark)
        assert "WARNING" in caplog.text
        assert "DBUtils import failure." in caplog.text
        assert dbutils is not None


def test_columns_except(sample_df: DataFrame):
    new_cols = common.columns_except(sample_df, ["x", "non-existing-col"])
    assert new_cols == ["y"]


def test_columns_except_empty(sample_df: DataFrame):
    new_cols = common.columns_except(sample_df)
    assert new_cols == ["x", "y"]


def test_dataframe_except_columns(sample_df: DataFrame):
    new_df = common.dataframe_except_columns(sample_df, ["x"])
    expected_df = sample_df.select("y")
    assert_df_equality(new_df, expected_df)


def test_dataframe_except_columns_not_existing(sample_df: DataFrame):
    with pytest.raises(RuntimeError):
        common.dataframe_except_columns(sample_df, ["non-existing-col"])


def test_get_table_postfix_is_empty():
    assert common.get_table_postfix("") == ""


def test_get_table_postfix_is_non_empty():
    assert common.get_table_postfix("1") == "_1"


def test_column_exists(sample_df: DataFrame):
    assert common.column_exists(sample_df, "x")
    assert common.column_exists(sample_df, "y")


def test_column_not_exists(sample_df: DataFrame):
    assert not common.column_exists(sample_df, "z")


def test_column_exists_case_sensitive(sample_df: DataFrame):
    assert common.column_exists(sample_df, "X")


def test_none_to_empty_str_empty():
    assert common.none_to_empty_str(None) == ""
    assert common.none_to_empty_str("") == ""


def test_none_to_empty_str_non_empty():
    assert common.none_to_empty_str("abc") == "abc"


def test_JinjaVarsHelper_non_existing_file():
    with pytest.raises(FileNotFoundError):
        common.JinjaVarsHelper("tests/unit/data/foobar.yml", "stage")


def test_JinjaVarsHelper_non_existing_env():
    with pytest.raises(KeyError):
        common.JinjaVarsHelper("tests/unit/data/jinja-vars.yml", "foobar")


def test_JinjaVarsHelper_get_s3_bucket():
    jinja_vars_helper = common.JinjaVarsHelper("tests/unit/data/jinja-vars.yml", "stage")
    assert jinja_vars_helper.get_s3_bucket() == "databricks-bucket-test"


def test_JinjaVarsHelper_get_db_data_schema():
    jinja_vars_helper = common.JinjaVarsHelper("tests/unit/data/jinja-vars.yml", "stage")
    assert jinja_vars_helper.get_db_data_schema() == "data_test"


def test_JinjaVarsHelper_get_ny_taxi_src_path_prod():
    jinja_vars_helper = common.JinjaVarsHelper("tests/unit/data/jinja-vars.yml", "prod")
    assert jinja_vars_helper.get_ny_taxi_src_path() == "/databricks-datasets/nyctaxi/sample/json/"


def test_JinjaVarsHelper_get_ny_taxi_src_path_test():
    github_run_id = "1"
    jinja_vars_helper = common.JinjaVarsHelper("tests/unit/data/jinja-vars.yml", "stage", github_run_id)
    assert (
        jinja_vars_helper.get_ny_taxi_src_path()
        == f"s3://databricks-bucket-test/data/input/{github_run_id}/uber/ny_taxi/"
    )


@pytest.mark.parametrize("test_input_env", ["stage", "prod"])
def test_validate_incoming_env_value(test_input_env):
    common.validate_incoming_env_value(test_input_env)


def test_validate_incoming_env_value_throws_exception():
    with pytest.raises(KeyError):
        common.validate_incoming_env_value("some_non_existing_value")
