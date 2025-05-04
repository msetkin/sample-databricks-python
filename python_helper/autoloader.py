from typing import Any, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.readwriter import DataStreamReader


class Autoloader:
    """
    A helper class that allows reading data from files in streaming mode.

    Supported formats:
        use read_json() method for json
    """

    def __init__(
        self,
        spark: SparkSession,
        input_path: str,
        select_columns: List[Any],
        partition_columns: str | None = None,
        allow_overwrites: bool = True,
        drop_columns: List[str] | None = None,
    ) -> None:
        """
        Args:
            spark:
                spark session to be used
            input_path:
                path to directory with data files
            select_columns:
                List of columns and column expressions to select from source
            partition_columns:
                Value for spark.readStream option cloudFiles.partitionColumns.
                Defaults to None.
            allow_overwrites:
                Value for spark.readStream.allowOverwrites. Defaults to True.
            drop_columns:
                List of columns to drop from source, including columns from
                <select_columns> argument. Non-existing column names will be
                silently ignored. Defaults to None.

        """

        self.spark = spark
        self.input_path = input_path
        self.select_columns = select_columns
        self.partition_columns = partition_columns
        self.allow_overwrites = allow_overwrites
        self.drop_columns = drop_columns

    def read_json(self) -> DataFrame:
        df_stream: DataStreamReader = self.spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")

        return self.__get_df(df_stream)

    def __get_df(self, dsr: DataStreamReader) -> DataFrame:
        dsr = dsr.option("cloudFiles.allowOverwrites", self.allow_overwrites)

        if self.partition_columns:
            dsr = dsr.option("cloudFiles.partitionColumns", self.partition_columns)

        df = dsr.load(self.input_path).select(self.select_columns)

        if self.drop_columns:
            return df.drop(*self.drop_columns)

        return df
