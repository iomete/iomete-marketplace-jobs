"""Tests for per-table/per-operation results recorded during a Data Compaction run.

The "unchanged behavior" tests pin what the job does today; they must pass before and after PR2.
"""

import logging
import re
import time
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row

import stats_emitter
from data_compaction_job.config import (
    ApplicationConfig,
    LockConfig,
    RewriteDataFilesConfig,
    RewriteManifestsConfig,
)
from data_compaction_job.sql_compaction import SqlCompaction

CATALOG = "spark_catalog"
SUMMARY = {"total-data-files": "10", "total-delete-files": "0", "total-files-size": "1000", "total-records": "10"}
HELD_LOCK = "ownerId=other;nonce=n;expiresAt=2999-01-01T00:00:00+00:00;version=1"


class ValidationException(Exception):
    pass


def rewrite_row(rewritten=0, added=0, rewritten_bytes=0, failed=0):
    return Row(rewritten_data_files_count=rewritten, added_data_files_count=added,
               rewritten_bytes_count=rewritten_bytes, failed_data_files_count=failed)


def snapshot(snapshot_id, operation="append", committed_at_ms=1_000, summary=None):
    return Row(snapshot_id=snapshot_id, operation=operation, committed_at_ms=committed_at_ms,
               summary=summary if summary is not None else dict(SUMMARY))


DEFAULT_RESULTS = {
    "rewrite_manifests": lambda wh: [Row(rewritten_manifests_count=0, added_manifests_count=0)],
    "rewrite_data_files": lambda wh: [rewrite_row()],
    "expire_snapshots": lambda wh: [Row(
        deleted_data_files_count=0, deleted_position_delete_files_count=0, deleted_equality_delete_files_count=0,
        deleted_manifest_files_count=0, deleted_manifest_lists_count=0, deleted_statistics_files_count=0)],
    "remove_orphan_files": lambda wh: [],
}


class Warehouse:
    def __init__(self, tables=("orders",), providers=None, results=None, lock_value=None,
                 state_error=False, describe_errors=None):
        self.tables = list(tables)
        self.providers = providers or {}
        self.results = dict(DEFAULT_RESULTS, **(results or {}))
        self.lock_value = lock_value
        self.state_error = state_error
        self.describe_errors = describe_errors or {}
        self.snapshots = {table: [snapshot(1)] for table in self.tables}
        self.queries = []
        self.state_reads = {}
        self.spark = MagicMock()
        self.spark.sql.side_effect = self.sql
        self.spark.sparkContext.applicationId = "app-test"

    def calls(self, procedure):
        return [query for query in self.queries if f"system.{procedure}(" in query]

    def sql(self, query):
        self.queries.append(query)
        rows = self._route(query)
        return MagicMock(collect=lambda: rows)

    def _route(self, query):
        normalized = query.strip().lower()
        names = re.findall(r"`([^`]+)`", query)
        if normalized.startswith("show databases"):
            return [Row(namespace="db")]
        if normalized.startswith("show tables"):
            return [Row(tableName=table) for table in self.tables]
        if normalized.startswith("create") or "iomete_system_db" in normalized:
            return []
        if normalized.startswith("describe extended"):
            table = names[2]
            if table in self.describe_errors:
                raise self.describe_errors[table]
            return [Row(col_name="Provider", data_type=self.providers.get(table, "iceberg"))]
        if normalized.startswith("show tblproperties") and "('current-snapshot-id')" in query:
            if self.state_error:
                raise RuntimeError("metadata unavailable")
            table = names[2]
            self.state_reads[table] = self.state_reads.get(table, 0) + 1
            current = max(row.snapshot_id for row in self.snapshots[table])
            return [Row(key="current-snapshot-id", value=str(current))]
        if ".snapshots" in query:
            return list(self.snapshots[names[2]])
        if normalized.startswith("show tblproperties"):
            return [Row(key="iomete.compaction.lock", value=self.lock_value)] if self.lock_value else []
        if normalized.startswith("alter table"):
            return []
        if normalized.startswith("call"):
            procedure = re.search(r"system\.(\w+)\(", query).group(1)
            result = self.results[procedure]
            if isinstance(result, Exception):
                raise result
            return result(self)
        raise AssertionError(f"unexpected query: {query}")


def make_config(**changes):
    config = ApplicationConfig(catalog=CATALOG)
    config.parallelism = 1
    config.include_exclude.table_include = []
    config.include_exclude.table_exclude = []
    for name, value in changes.items():
        setattr(config, name, value)
    return config


def run(warehouse, config=None):
    job = SqlCompaction(warehouse.spark, config or make_config())
    with patch("data_compaction_job.sql_compaction.SqlClient.catalogs", return_value={CATALOG}):
        job.run_compaction()
    return job


def table_result(job, name="orders"):
    return next(table for table in job.run_result.tables if table.table == name)


def operation_result(job, operation, table="orders"):
    return next(result for result in table_result(job, table).operations if result.operation == operation)


def test_unchanged_procedure_failure_is_recorded_as_error_row_and_later_operations_run():
    warehouse = Warehouse(results={"rewrite_data_files": ValidationException("Missing required files to delete: x")})

    with patch.object(stats_emitter.StatsBatcher, "add_error", autospec=True) as add_error:
        run(warehouse)

    assert add_error.call_count == 1
    kwargs = add_error.call_args.kwargs
    assert kwargs["operation"] == "REWRITE_DATA_FILES"
    assert kwargs["error"] == "Missing required files to delete: x"
    assert kwargs["table_metadata"].table == "orders"
    assert warehouse.calls("expire_snapshots") and warehouse.calls("remove_orphan_files")


def test_unchanged_metrics_row_for_a_successful_rewrite():
    warehouse = Warehouse(results={"rewrite_data_files": lambda wh: [rewrite_row(40, 2, 1024, 0)]})

    with patch.object(stats_emitter.StatsBatcher, "add_metric", autospec=True) as add_metric:
        run(warehouse)

    rewrite_calls = [c for c in add_metric.call_args_list if c.kwargs["operation"] == "REWRITE_DATA_FILES"]
    assert len(rewrite_calls) == 1
    assert rewrite_calls[0].kwargs["metrics"] == {
        "rewritten_data_files_count": "40", "added_data_files_count": "2",
        "rewritten_bytes_count": "1024", "failed_data_files_count": "0",
    }
    assert rewrite_calls[0].kwargs["query"] == warehouse.calls("rewrite_data_files")[0]


def test_unchanged_run_does_not_raise_when_an_operation_fails():
    warehouse = Warehouse(results={"rewrite_data_files": ValidationException("boom")})

    run(warehouse)


def test_unchanged_zero_row_result_still_stops_the_remaining_operations():
    warehouse = Warehouse(results={"rewrite_manifests": lambda wh: []})

    run(warehouse)

    assert warehouse.calls("rewrite_manifests")
    assert not warehouse.calls("rewrite_data_files")
    assert not warehouse.calls("expire_snapshots")
    assert not warehouse.calls("remove_orphan_files")


def test_unchanged_disabled_operation_issues_no_procedure_call():
    warehouse = Warehouse()

    run(warehouse, make_config(rewrite_manifests=RewriteManifestsConfig(enabled=False)))

    assert not warehouse.calls("rewrite_manifests")
    assert warehouse.calls("rewrite_data_files")


def test_records_one_result_per_discovered_table():
    job = run(Warehouse(tables=("orders", "events")))

    assert job.run_result.tables_discovered == 2
    assert sorted(table.table for table in job.run_result.tables) == ["events", "orders"]
    assert job.run_result.spark_app_id == "app-test"
    assert job.run_result.catalog == CATALOG


def test_records_disabled_operation():
    job = run(Warehouse(), make_config(rewrite_manifests=RewriteManifestsConfig(enabled=False)))

    result = operation_result(job, "REWRITE_MANIFESTS")
    assert result.status == "DISABLED"
    assert result.reason == "Disabled by configuration."


def test_records_every_operation_once():
    # Ordering belongs to PR1; only membership is pinned here.
    job = run(Warehouse())

    assert sorted(result.operation for result in table_result(job).operations) == [
        "EXPIRE_SNAPSHOTS", "REMOVE_ORPHAN_FILES", "REWRITE_DATA_FILES", "REWRITE_MANIFESTS"]


def test_non_iceberg_table_is_skipped():
    job = run(Warehouse(tables=("raw_view",), providers={"raw_view": "hive"}))

    table = table_result(job, "raw_view")
    assert table.status == "SKIPPED"
    assert table.skip_reason_code == "not_iceberg"
    assert table.operations == []


def test_table_with_held_lock_is_skipped():
    job = run(Warehouse(lock_value=HELD_LOCK), make_config(lock=LockConfig(enabled=True)))

    table = table_result(job)
    assert table.status == "SKIPPED"
    assert table.skip_reason_code == "lock_held"


def test_procedure_failure_is_recorded_with_error_and_query():
    warehouse = Warehouse(results={"rewrite_data_files": ValidationException("Missing required files to delete: x")})

    job = run(warehouse)

    result = operation_result(job, "REWRITE_DATA_FILES")
    assert result.status == "FAILED"
    assert result.reason_code == "procedure_error"
    assert result.error_type == "ValidationException"
    assert result.error_message == "Missing required files to delete: x"
    assert result.query == warehouse.calls("rewrite_data_files")[0]
    assert operation_result(job, "EXPIRE_SNAPSHOTS").status == "NO_WORK"


def test_zero_rewrite_without_partial_progress_is_no_work():
    job = run(Warehouse(), make_config(rewrite_data_files=RewriteDataFilesConfig(options={"min-input-files": 2})))

    result = operation_result(job, "REWRITE_DATA_FILES")
    assert result.status == "NO_WORK"
    assert result.reason == "No files required rewriting."


def test_zero_rewrite_under_partial_progress_is_unverified_and_reports_window_evidence():
    def rewrite_with_concurrent_writer(warehouse):
        warehouse.snapshots["orders"].append(snapshot(2, "append", int(time.time() * 1000) + 1))
        time.sleep(0.01)
        return [rewrite_row()]

    warehouse = Warehouse(results={"rewrite_data_files": rewrite_with_concurrent_writer})
    options = {"min-input-files": 2, "partial-progress.enabled": True, "partial-progress.max-commits": 20}

    job = run(warehouse, make_config(rewrite_data_files=RewriteDataFilesConfig(options=options)))

    result = operation_result(job, "REWRITE_DATA_FILES")
    assert result.status == "UNVERIFIED"
    assert result.reason_code == "zero_committed_under_partial_progress"
    assert "Non-replace snapshots observed during the operation window: 1 (append 1)." in result.notes


def test_table_state_is_read_once_before_and_once_after():
    warehouse = Warehouse()

    job = run(warehouse)

    table = table_result(job)
    assert warehouse.state_reads == {"orders": 2}
    assert table.before.snapshot_id == 1
    assert table.before.data_files == 10
    assert table.after.snapshot_id == 1


def test_state_read_failure_does_not_change_execution():
    warehouse = Warehouse(state_error=True)

    job = run(warehouse)

    table = table_result(job)
    assert table.before is None and table.after is None
    for procedure in ("rewrite_manifests", "rewrite_data_files", "expire_snapshots", "remove_orphan_files"):
        assert warehouse.calls(procedure)
    assert operation_result(job, "EXPIRE_SNAPSHOTS").reason_code == "table_state_unavailable"


def test_effective_options_and_sql_are_recorded_verbatim():
    options = {"min-input-files": 2, "future-iceberg-option": "x"}
    warehouse = Warehouse()

    job = run(warehouse, make_config(rewrite_data_files=RewriteDataFilesConfig(options=options)))

    result = operation_result(job, "REWRITE_DATA_FILES")
    assert result.options == options
    assert result.query == warehouse.calls("rewrite_data_files")[0]
    assert "'future-iceberg-option', 'x'" in result.query


def test_duration_is_recorded():
    job = run(Warehouse())

    result = operation_result(job, "REWRITE_DATA_FILES")
    assert result.started_at is not None
    assert result.ended_at >= result.started_at
    assert result.duration_seconds >= 0


def test_zero_row_result_is_recorded_as_unreadable_and_later_operations_as_not_attempted():
    job = run(Warehouse(results={"rewrite_manifests": lambda wh: []}))

    table = table_result(job)
    assert operation_result(job, "REWRITE_MANIFESTS").status == "FAILED"
    assert operation_result(job, "REWRITE_MANIFESTS").reason_code == "result_unreadable"
    for operation in ("REWRITE_DATA_FILES", "EXPIRE_SNAPSHOTS", "REMOVE_ORPHAN_FILES"):
        assert operation_result(job, operation).status == "SKIPPED"
        assert operation_result(job, operation).reason_code == "not_attempted"
    assert table.status == "FAILED"


def test_describe_failure_is_a_table_level_failure():
    warehouse = Warehouse(describe_errors={"orders": RuntimeError("catalog unreachable")})

    job = run(warehouse)

    table = table_result(job)
    assert table.status == "FAILED"
    assert table.error_message == "catalog unreachable"
    assert table.operations == []


def test_run_status_is_the_most_severe_table_status():
    warehouse = Warehouse(tables=("orders", "events"),
                          results={"rewrite_data_files": ValidationException("boom")})

    job = run(warehouse)

    assert job.run_result.status == "FAILED"


def test_summary_is_logged_once_at_the_end(caplog):
    from data_compaction_job.compaction_summary import render_summary

    caplog.set_level(logging.INFO, logger="data_compaction_job.sql_compaction")

    job = run(Warehouse())

    summaries = [record.getMessage() for record in caplog.records if "DATA COMPACTION SUMMARY" in record.getMessage()]
    assert summaries == ["\n" + render_summary(job.run_result)]


def test_summary_rendering_failure_never_breaks_the_run():
    with patch("data_compaction_job.sql_compaction.render_summary",
               side_effect=RuntimeError("render bug"), create=True):
        run(Warehouse())


GLOBAL_OPTIONS = {"min-input-files": 2, "partial-progress.enabled": True}


def rewrite_run(table_overrides):
    warehouse = Warehouse()
    config = make_config(rewrite_data_files=RewriteDataFilesConfig(options=dict(GLOBAL_OPTIONS)),
                         table_overrides=table_overrides)
    return run(warehouse, config), warehouse


def test_unchanged_global_options_are_rendered_when_there_is_no_override():
    _, warehouse = rewrite_run(None)

    assert "options => map('min-input-files', '2', 'partial-progress.enabled', 'true')" in \
        warehouse.calls("rewrite_data_files")[0]


def test_unchanged_override_options_replace_global_options():
    _, warehouse = rewrite_run({"db.orders": {"rewrite_data_files": {"options": {"rewrite-all": True}}}})

    assert "options => map('rewrite-all', 'true')" in warehouse.calls("rewrite_data_files")[0]


def test_unchanged_empty_override_options_fall_back_to_global_options():
    _, warehouse = rewrite_run({"db.orders": {"rewrite_data_files": {"options": {}}}})

    assert "options => map('min-input-files', '2', 'partial-progress.enabled', 'true')" in \
        warehouse.calls("rewrite_data_files")[0]


def test_unchanged_override_option_keys_are_not_cleaned():
    _, warehouse = rewrite_run({"db.orders": {"rewrite_data_files": {"options": {'"quoted.key"': 1}}}})

    assert "options => map('\"quoted.key\"', '1')" in warehouse.calls("rewrite_data_files")[0]


@pytest.mark.parametrize("table_overrides, expected", [
    (None, GLOBAL_OPTIONS),
    ({"db.orders": {"rewrite_data_files": {"options": {"rewrite-all": True}}}}, {"rewrite-all": True}),
    ({"db.orders": {"rewrite_data_files": {"options": {}}}}, GLOBAL_OPTIONS),
])
def test_recorded_options_match_what_was_sent(table_overrides, expected):
    job, _ = rewrite_run(table_overrides)

    assert operation_result(job, "REWRITE_DATA_FILES").options == expected


def test_concurrent_lookups_create_exactly_one_result_per_table():
    from concurrent.futures import ThreadPoolExecutor

    from data_compaction_job.compaction_results import TableResult
    from data_compaction_job.config import TableMetadata

    def slow_table_result(*args, **kwargs):
        time.sleep(0.01)
        return TableResult(*args, **kwargs)

    job = SqlCompaction(MagicMock(), make_config())
    tables = [f"t{i}" for i in range(20)]
    lookups = [TableMetadata(catalog=CATALOG, database="db", table=table) for table in tables for _ in range(5)]

    with patch("data_compaction_job.sql_compaction.TableResult", side_effect=slow_table_result), \
            ThreadPoolExecutor(max_workers=16) as pool:
        returned = list(pool.map(job._SqlCompaction__table_result, lookups))

    assert sorted(table.table for table in job.run_result.tables) == sorted(tables)
    by_table = {}
    for metadata, result in zip(lookups, returned):
        assert by_table.setdefault(metadata.table, result) is result


def test_parallel_run_records_every_table_once_with_all_operations():
    tables = [f"t{i}" for i in range(30)]

    job = run(Warehouse(tables=tables), make_config(parallelism=8))

    assert sorted(table.table for table in job.run_result.tables) == sorted(tables)
    assert all(len(table.operations) == 4 for table in job.run_result.tables)
