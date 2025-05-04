from python_helper.import_dlt import dlt


def test_view_decorator():
    @dlt.view(name="test_view", comment="A test view")
    def dummy_view_function():
        pass

    assert callable(dummy_view_function)


def test_table_decorator():
    @dlt.table(name="test_table", comment="A test table")
    def dummy_function():
        pass

    assert callable(dummy_function)


def test_create_streaming_table():
    dlt.create_streaming_table(
        name="test_streaming_table",
        comment="A test streaming table",
        spark_conf={"spark.executor.memory": "2g"},
        table_properties={"delta.autoOptimize.optimizeWrite": "true"},
        partition_cols=["date"],
        cluster_by=["id"],
        path="/mnt/data/test_streaming_table",
        schema="id INT, value STRING",
        expect_all={"id": "NOT NULL"},
        expect_all_or_drop={"value": "NOT NULL"},
        expect_all_or_fail={"id": "NOT NULL"},
        row_filter="id > 0",
    )


def test_apply_changes():
    dlt.apply_changes(
        target="target_table",
        source="source_table",
        keys=["id"],
        sequence_by="timestamp",
        ignore_null_updates=True,
        apply_as_deletes=None,
        apply_as_truncates=None,
        column_list=["id", "value"],
        except_column_list=None,
        stored_as_scd_type="type2",
        track_history_column_list=None,
        track_history_except_column_list=None,
    )
