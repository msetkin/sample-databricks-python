from typing import Any, Callable, Dict, List

from pyspark.sql import Column

try:
    import dlt
except ImportError:
    # we need to ignore type errors here because mypy believes we are redefining dlt class
    class dlt:  # type: ignore
        @staticmethod
        def table(name: str, comment: str | None = None, **options: Any) -> Callable[[Callable], Callable]:
            def _(f: Callable) -> Callable:
                return f

            return _

        @staticmethod
        def create_streaming_table(
            name: str = "<table-name>",
            comment: str = "<comment>",
            spark_conf: Dict[str, str] = {"<key>": "<value>"},
            table_properties: Dict[str, str] = {"<key>": "<value>"},
            partition_cols: List[str] = ["<partition-column1>", "<partition-column2>"],
            cluster_by: List[str] = ["<clustering-column1>", "<clustering-column2>"],
            path: str = "<storage-location-path>",
            schema: str = "schema-definition",
            expect_all: Dict[str, str] = {"<key>": "<value>"},
            expect_all_or_drop: Dict[str, str] = {"<key>": "<value>"},
            expect_all_or_fail: Dict[str, str] = {"<key>": "<value>"},
            row_filter: str = "row-filter-clause",
        ) -> None:
            return None

        @staticmethod
        def apply_changes(
            target: str = "<target-table>",
            source: str = "<data-source>",
            keys: List[str] = ["key1", "key2", "keyN"],
            sequence_by: str = "<sequence-column>",
            ignore_null_updates: bool = False,
            apply_as_deletes: Column | None = None,
            apply_as_truncates: Column | None = None,
            column_list: List[str] | None = None,
            except_column_list: List[str] | None = None,
            stored_as_scd_type: str | None = None,
            track_history_column_list: List[str] | None = None,
            track_history_except_column_list: List[str] | None = None,
        ) -> None:
            return None

        @staticmethod
        def view(name: str = "<name>", comment: str = "<comment>") -> Callable[[Callable], Callable]:
            def _(f: Callable) -> Callable:
                return f

            return _
