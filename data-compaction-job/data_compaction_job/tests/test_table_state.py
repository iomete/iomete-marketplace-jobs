"""Tests for reading lightweight table state from Iceberg snapshot metadata."""

import re
from unittest.mock import MagicMock

from pyspark.sql import Row

from data_compaction_job.compaction_results import SnapshotInfo
from data_compaction_job.compaction_table_state import read_table_state


class MetadataUnavailable(Exception):
    pass


SUMMARY = {
    "total-data-files": "40",
    "total-delete-files": "3",
    "total-files-size": "1048576",
    "total-records": "400",
}


def fake_spark(current_snapshot_id, snapshot_rows):
    spark = MagicMock()
    queries = []

    def sql(query):
        queries.append(query)
        if query.startswith("SHOW TBLPROPERTIES"):
            rows = [Row(key="current-snapshot-id", value=str(current_snapshot_id))]
        elif ".snapshots" in query:
            current = re.search(r"WHERE snapshot_id = (\d+)", query)
            rows = [row for row in snapshot_rows if current is None or row.snapshot_id == int(current.group(1))]
        else:
            raise AssertionError(f"unexpected query: {query}")
        return MagicMock(collect=lambda: rows)

    spark.sql.side_effect = sql
    return spark, queries


def snapshot(snapshot_id, operation, committed_at_ms, summary=None):
    return Row(snapshot_id=snapshot_id, operation=operation, committed_at_ms=committed_at_ms,
               summary=summary or {})


def test_reads_current_snapshot_summary_totals():
    spark, _ = fake_spark(101, [snapshot(100, "append", 1_000), snapshot(101, "replace", 2_000, SUMMARY)])

    table_state = read_table_state(spark, "spark_catalog", "db", "orders")

    assert table_state.snapshot_id == 101
    assert table_state.data_files == 40
    assert table_state.delete_files == 3
    assert table_state.total_files_size == 1048576
    assert table_state.records == 400


def test_totals_come_from_the_current_snapshot_even_when_it_is_not_the_latest():
    rolled_back = dict(SUMMARY, **{"total-data-files": "7"})
    spark, _ = fake_spark(100, [snapshot(100, "append", 1_000, rolled_back), snapshot(101, "replace", 2_000, SUMMARY)])

    table_state = read_table_state(spark, "spark_catalog", "db", "orders")

    assert table_state.snapshot_id == 100
    assert table_state.data_files == 7


def test_captures_every_snapshot_with_operation_and_commit_time():
    spark, _ = fake_spark(101, [snapshot(100, "append", 1_000), snapshot(101, "replace", 2_000, SUMMARY)])

    table_state = read_table_state(spark, "spark_catalog", "db", "orders")

    assert table_state.snapshots == [SnapshotInfo(100, "append", 1_000), SnapshotInfo(101, "replace", 2_000)]


def test_missing_summary_fields_are_unavailable_not_zero():
    spark, _ = fake_spark(101, [snapshot(101, "append", 2_000, {"total-data-files": "5"})])

    table_state = read_table_state(spark, "spark_catalog", "db", "orders")

    assert table_state.data_files == 5
    assert table_state.delete_files is None
    assert table_state.total_files_size is None
    assert table_state.records is None


def test_table_without_snapshots():
    spark, queries = fake_spark("none", [])

    table_state = read_table_state(spark, "spark_catalog", "db", "empty")

    assert table_state.snapshot_id is None
    assert table_state.data_files is None
    assert table_state.snapshots == []
    assert not any("WHERE" in query for query in queries)


def test_read_failure_returns_none_instead_of_raising():
    spark = MagicMock()
    spark.sql.side_effect = MetadataUnavailable("metadata unavailable")

    assert read_table_state(spark, "spark_catalog", "db", "orders") is None


def test_reads_only_the_current_snapshot_summary_with_quoted_identifiers():
    spark, queries = fake_spark(101, [snapshot(101, "append", 2_000, SUMMARY)])

    read_table_state(spark, "spark_catalog", "db", "orders")

    assert queries == [
        "SHOW TBLPROPERTIES `spark_catalog`.`db`.`orders` ('current-snapshot-id')",
        "SELECT snapshot_id, operation, unix_millis(committed_at) AS committed_at_ms "
        "FROM `spark_catalog`.`db`.`orders`.snapshots",
        "SELECT summary FROM `spark_catalog`.`db`.`orders`.snapshots WHERE snapshot_id = 101",
    ]
