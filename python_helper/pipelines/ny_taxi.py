from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    input_file_name,
    substring_index,
)

from python_helper.autoloader import Autoloader
from python_helper.common import (
    JinjaVarsHelper,
    get_physical_table_name,
    get_spark_session,
)
from python_helper.import_dlt import dlt
from python_helper.pipelines.pipeline import Pipeline
from python_helper.transformations.ny_taxi import (
    get_silver_ny_taxi_df,
)


class NY_Taxi(Pipeline):
    def __init__(self, jinja_vars_helper: JinjaVarsHelper, github_run_id: str = ""):
        super().__init__()
        self.__spark = get_spark_session(None)
        self.__db_schema = jinja_vars_helper.get_db_data_schema()
        self.__input_path = jinja_vars_helper.get_ny_taxi_src_path()
        self.__bronze_table = f"{self.__db_schema}.{get_physical_table_name('bronze_ny_taxi', github_run_id)}"
        self.__silver_table = f"{self.__db_schema}.{get_physical_table_name('silver_ny_taxi', github_run_id)}"

    def define_bronze_pipelines(self) -> None:
        csv_loader = Autoloader(
            spark=self.__spark,
            input_path=self.__input_path,
            select_columns=[
                "pep_pickup_date_txt",
                "DOLocationID",
                "PULocationID",
                "RatecodeID",
                "VendorID",
                "congestion_surcharge",
                "extra",
                "fare_amount",
                "improvement_surcharge",
                "mta_tax",
                "passenger_count",
                "payment_type",
                "store_and_fwd_flag",
                "tip_amount",
                "tolls_amount",
                "total_amount",
                "tpep_dropoff_datetime",
                "tpep_pickup_datetime",
                "trip_distance",
                date_format(current_timestamp(), "yyyy-MM-dd").alias("_processing_date"),
                current_timestamp().alias("_processing_ts"),
                substring_index(input_file_name(), "/", -1).alias("_file_name"),
            ],
            partition_columns="pep_pickup_date_txt",
            drop_columns=["_rescued_data"],
        )

        @dlt.table(
            name=self.__bronze_table,
            comment="Bronze NY taxi data incrementally ingested from S3 object storage",
        )
        def bronze_ny_taxi_table() -> DataFrame:
            return csv_loader.read_json()

    def define_silver_pipelines(self) -> None:
        self.__define_silver_pipelines_dlt_ny_taxi()
        self.__define_silver_pipelines_dlt_ny_taxi_scd1()

    def __define_silver_pipelines_dlt_ny_taxi(self) -> None:
        @dlt.table(
            name=self.__silver_table,
            comment="Silver NY taxi data",
        )
        def table() -> DataFrame:
            bronze_cva_dva = dlt.read_stream(self.__bronze_table)
            return get_silver_ny_taxi_df(bronze_cva_dva)

    def __define_silver_pipelines_dlt_ny_taxi_scd1(self) -> None:
        dlt.create_streaming_table(
            name=f"{self.__silver_table}_scd1",
            comment="Silver NY taxi current state data (in case there were updates)",
        )

        dlt.apply_changes(
            target=f"{self.__silver_table}_scd1",
            source=self.__silver_table,
            keys=["DOLocationID", "PULocationID", "VendorID"],
            sequence_by=col("tpep_pickup_datetime"),
            track_history_except_column_list=[
                "_row_processing_ts",
                "_processing_ts",
                "_processing_date",
            ],
            stored_as_scd_type=1,
        )
