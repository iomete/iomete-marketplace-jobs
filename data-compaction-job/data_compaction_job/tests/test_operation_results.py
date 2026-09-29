"""Tests for classifying one maintenance operation from what Iceberg returned."""

import pytest
from pyspark.sql import Row

from data_compaction_job.compaction_results import (
    OperationResult,
    OperationSpec,
    OperationStatus,
    ProcedureOutcome,
    RunResult,
    SnapshotInfo,
    TableResult,
    TableState,
    classify_operation,
    disabled_operation,
    partial_progress_enabled,
    skipped_operation,
)

REWRITE_SQL = "CALL `spark_catalog`.system.rewrite_data_files(table => '`spark_catalog`.`db`.`t`')"


def rewrite_row(rewritten=0, added=0, rewritten_bytes=0, failed=0, **extra):
    return Row(
        rewritten_data_files_count=rewritten,
        added_data_files_count=added,
        rewritten_bytes_count=rewritten_bytes,
        failed_data_files_count=failed,
        **extra,
    )


def outcome(rows=None, error=None, query=REWRITE_SQL, started_at=100.0, ended_at=110.0):
    return ProcedureOutcome(query=query, rows=rows, error=error, started_at=started_at, ended_at=ended_at)


def state(snapshot_id, snapshots=None, data_files=None):
    return TableState(snapshot_id=snapshot_id, data_files=data_files, snapshots=snapshots)


PP_ON = {"min-input-files": 2, "partial-progress.enabled": True}
PP_OFF = {"min-input-files": 2}


class TestRewriteDataFiles:
    def test_committed_rewrite_without_partial_progress_is_success_with_work(self):
        result = classify_operation(
            "REWRITE_DATA_FILES", outcome([rewrite_row(rewritten=40, added=2, rewritten_bytes=1024)]), PP_OFF)

        assert result.status == OperationStatus.SUCCESS_WITH_WORK
        assert result.reason_code == "committed"
        assert result.reason == "Iceberg committed a rewrite of 40 data file(s) into 2 file(s)."
        assert not any(note.startswith("Partial progress is enabled") for note in result.notes)

    def test_committed_rewrite_with_partial_progress_notes_hidden_commit_failures(self):
        result = classify_operation(
            "REWRITE_DATA_FILES", outcome([rewrite_row(rewritten=40, added=2)]), PP_ON)

        assert result.status == OperationStatus.SUCCESS_WITH_WORK
        assert any(note.startswith("Partial progress is enabled") for note in result.notes)

    def test_zero_counters_without_partial_progress_is_no_work(self):
        # Without partial progress a failed commit raises instead of returning zeros.
        result = classify_operation("REWRITE_DATA_FILES", outcome([rewrite_row()]), PP_OFF)

        assert result.status == OperationStatus.NO_WORK
        assert result.reason_code == "no_files_required_rewriting"
        assert result.reason == "No files required rewriting."

    def test_zero_counters_with_partial_progress_is_unverified(self):
        result = classify_operation("REWRITE_DATA_FILES", outcome([rewrite_row()]), PP_ON)

        assert result.status == OperationStatus.UNVERIFIED
        assert result.reason_code == "zero_committed_under_partial_progress"
        assert "zero committed rewrite metrics" in result.reason
        assert "cannot tell the two apart" in result.reason

    def test_zero_counters_with_partial_progress_stay_unverified_when_snapshot_is_unchanged(self):
        before = state(500, [SnapshotInfo(500, "append", 1_000)])
        after = state(500, [SnapshotInfo(500, "append", 1_000)])

        result = classify_operation("REWRITE_DATA_FILES", outcome([rewrite_row()]), PP_ON, before, after)

        assert result.status == OperationStatus.UNVERIFIED

    def test_partial_progress_with_zero_max_failed_commits_proves_no_work(self):
        # Iceberg throws when failed commits exceed partial-progress.max-failed-commits.
        options = {"partial-progress.enabled": "true", "partial-progress.max-failed-commits": "0"}

        result = classify_operation("REWRITE_DATA_FILES", outcome([rewrite_row()]), options)

        assert result.status == OperationStatus.NO_WORK

    @pytest.mark.parametrize("value, expected", [
        (True, True), ("true", True), ("TRUE", True), (False, False), ("false", False), ("yes", False),
    ])
    def test_partial_progress_value_follows_java_boolean_parsing(self, value, expected):
        assert partial_progress_enabled({"partial-progress.enabled": value}) is expected

    @pytest.mark.parametrize("options", [None, {}, {"min-input-files": 2}])
    def test_partial_progress_defaults_to_disabled(self, options):
        assert partial_progress_enabled(options) is False

    def test_failed_groups_with_committed_files_is_partial(self):
        result = classify_operation(
            "REWRITE_DATA_FILES", outcome([rewrite_row(rewritten=10, added=1, failed=5)]), PP_ON)

        assert result.status == OperationStatus.PARTIAL
        assert result.reason_code == "some_groups_failed"

    def test_failed_groups_with_nothing_committed_is_failed(self):
        result = classify_operation("REWRITE_DATA_FILES", outcome([rewrite_row(failed=5)]), PP_ON)

        assert result.status == OperationStatus.FAILED
        assert result.reason_code == "rewrite_groups_failed"

    def test_removed_dangling_deletes_alone_counts_as_work(self):
        result = classify_operation(
            "REWRITE_DATA_FILES", outcome([rewrite_row(removed_delete_files_count=3)]), PP_OFF)

        assert result.status == OperationStatus.SUCCESS_WITH_WORK

    def test_procedure_error_is_failed_and_preserves_error_and_query(self):
        class ValidationException(Exception):
            pass

        error = ValidationException("Missing required files to delete: s3://b/f1.parquet")

        result = classify_operation("REWRITE_DATA_FILES", outcome(error=error), PP_ON)

        assert result.status == OperationStatus.FAILED
        assert result.reason_code == "procedure_error"
        assert result.error_type == "ValidationException"
        assert result.error_message == "Missing required files to delete: s3://b/f1.parquet"
        assert result.query == REWRITE_SQL
        assert result.metrics is None

    def test_raw_metrics_are_preserved_exactly(self):
        row = rewrite_row(rewritten=1, added=1, rewritten_bytes=7, failed=0, some_future_column=42)

        result = classify_operation("REWRITE_DATA_FILES", outcome([row]), PP_OFF)

        assert result.metrics == row.asDict()

    def test_effective_options_are_kept_verbatim_without_an_allow_list(self):
        options = {"min-input-files": 2, "future-iceberg-option": "x", "partial-progress.enabled": True}

        result = classify_operation("REWRITE_DATA_FILES", outcome([rewrite_row()]), options)

        assert result.options == options

    def test_query_and_timing_are_recorded(self):
        result = classify_operation(
            "REWRITE_DATA_FILES", outcome([rewrite_row()], started_at=100.0, ended_at=162.5), PP_OFF)

        assert result.query == REWRITE_SQL
        assert result.started_at == 100.0
        assert result.ended_at == 162.5
        assert result.duration_seconds == 62.5


class TestOperationWindowEvidence:
    def test_non_replace_snapshots_in_the_window_are_reported_as_observations(self):
        before = state(500, [SnapshotInfo(500, "append", 50_000)])
        after = state(502, [
            SnapshotInfo(500, "append", 50_000),
            SnapshotInfo(501, "append", 105_000),
            SnapshotInfo(502, "delete", 106_000),
        ])

        result = classify_operation(
            "REWRITE_DATA_FILES", outcome([rewrite_row()], started_at=100.0, ended_at=110.0), PP_ON, before, after)

        assert result.status == OperationStatus.UNVERIFIED
        assert "Non-replace snapshots observed during the operation window: 2 (append 1, delete 1)." in result.notes

    def test_replace_snapshots_in_the_window_are_not_attributed(self):
        before = state(500, [SnapshotInfo(500, "append", 50_000)])
        after = state(501, [SnapshotInfo(500, "append", 50_000), SnapshotInfo(501, "replace", 105_000)])

        result = classify_operation(
            "REWRITE_DATA_FILES", outcome([rewrite_row()], started_at=100.0, ended_at=110.0), PP_ON, before, after)

        assert "Replace snapshots observed during the operation window: 1; attribution unavailable." in result.notes
        assert not any(note.startswith("Non-replace") for note in result.notes)

    def test_snapshots_outside_the_window_add_no_note(self):
        before = state(500, [SnapshotInfo(500, "append", 50_000)])
        after = state(501, [SnapshotInfo(500, "append", 50_000), SnapshotInfo(501, "append", 200_000)])

        result = classify_operation(
            "REWRITE_DATA_FILES", outcome([rewrite_row()], started_at=100.0, ended_at=110.0), PP_ON, before, after)

        assert not any("operation window" in note for note in result.notes)

    def test_observations_never_claim_a_cause(self):
        before = state(500, [SnapshotInfo(500, "append", 50_000)])
        after = state(501, [SnapshotInfo(500, "append", 50_000), SnapshotInfo(501, "delete", 105_000)])

        result = classify_operation(
            "REWRITE_DATA_FILES", outcome([rewrite_row()], started_at=100.0, ended_at=110.0), PP_ON, before, after)

        text = " ".join([result.reason] + result.notes).lower()
        assert "because" not in text
        assert "concurrent modification" not in text
        assert "other writers" not in text


class TestRewriteManifests:
    def test_zero_counters_are_no_work(self):
        row = Row(rewritten_manifests_count=0, added_manifests_count=0)

        result = classify_operation("REWRITE_MANIFESTS", outcome([row]))

        assert result.status == OperationStatus.NO_WORK
        assert result.reason == "No manifests required rewriting."

    def test_rewritten_manifests_are_work(self):
        row = Row(rewritten_manifests_count=8, added_manifests_count=1)

        result = classify_operation("REWRITE_MANIFESTS", outcome([row]))

        assert result.status == OperationStatus.SUCCESS_WITH_WORK
        assert result.reason == "Iceberg rewrote 8 manifest(s) into 1 manifest(s)."


def expire_row(**counts):
    columns = {
        "deleted_data_files_count": 0,
        "deleted_position_delete_files_count": 0,
        "deleted_equality_delete_files_count": 0,
        "deleted_manifest_files_count": 0,
        "deleted_manifest_lists_count": 0,
        "deleted_statistics_files_count": 0,
    }
    columns.update(counts)
    return Row(**columns)


class TestExpireSnapshots:
    def test_deleted_files_are_work(self):
        result = classify_operation("EXPIRE_SNAPSHOTS", outcome([expire_row(deleted_manifest_lists_count=3)]))

        assert result.status == OperationStatus.SUCCESS_WITH_WORK

    def test_zero_counters_with_no_snapshot_removed_is_no_work(self):
        snapshots = [SnapshotInfo(1, "append", 1_000), SnapshotInfo(2, "append", 2_000)]

        result = classify_operation(
            "EXPIRE_SNAPSHOTS", outcome([expire_row()]), None, state(2, snapshots), state(2, snapshots))

        assert result.status == OperationStatus.NO_WORK
        assert result.reason == "No snapshots were eligible for expiration."

    def test_zero_counters_with_a_snapshot_removed_is_unverified(self):
        before = state(2, [SnapshotInfo(1, "append", 1_000), SnapshotInfo(2, "append", 2_000)])
        after = state(2, [SnapshotInfo(2, "append", 2_000)])

        result = classify_operation("EXPIRE_SNAPSHOTS", outcome([expire_row()]), None, before, after)

        assert result.status == OperationStatus.UNVERIFIED
        assert result.reason_code == "snapshots_removed_without_deleted_files"

    def test_zero_counters_without_table_state_is_unverified(self):
        result = classify_operation("EXPIRE_SNAPSHOTS", outcome([expire_row()]), None, None, None)

        assert result.status == OperationStatus.UNVERIFIED
        assert result.reason_code == "table_state_unavailable"


class TestRemoveOrphanFiles:
    def test_no_rows_is_no_work(self):
        result = classify_operation("REMOVE_ORPHAN_FILES", outcome([]))

        assert result.status == OperationStatus.NO_WORK
        assert result.reason == "No orphan files were found."
        assert result.row_count == 0

    def test_rows_are_work(self):
        rows = [Row(orphan_file_location=f"s3://b/orphan-{i}") for i in range(3)]

        result = classify_operation("REMOVE_ORPHAN_FILES", outcome(rows))

        assert result.status == OperationStatus.SUCCESS_WITH_WORK
        assert result.row_count == 3
        assert result.reason == "Iceberg returned 3 orphan file location(s)."


class TestGenericOperations:
    def test_unknown_operation_with_zero_counters_is_unverified(self):
        result = classify_operation("SOME_FUTURE_OPERATION", outcome([Row(things_count=0)]))

        assert result.status == OperationStatus.UNVERIFIED
        assert result.reason_code == "unknown_operation_semantics"

    def test_unknown_operation_with_positive_counters_is_work(self):
        result = classify_operation("SOME_FUTURE_OPERATION", outcome([Row(things_count=2)]))

        assert result.status == OperationStatus.SUCCESS_WITH_WORK

    def test_a_future_operation_spec_uses_the_same_rules(self):
        spec = OperationSpec(
            name="REWRITE_POSITION_DELETE_FILES",
            work_counters=("rewritten_delete_files_count", "added_delete_files_count"),
            no_work_reason="No delete files required rewriting.",
            commit_failures_hidden=partial_progress_enabled,
        )
        row = Row(rewritten_delete_files_count=0, added_delete_files_count=0,
                  rewritten_bytes_count=0, added_bytes_count=0)

        hidden = classify_operation(spec.name, outcome([row]), {"partial-progress.enabled": "true"}, spec=spec)
        proven = classify_operation(spec.name, outcome([row]), {}, spec=spec)

        assert hidden.status == OperationStatus.UNVERIFIED
        assert proven.status == OperationStatus.NO_WORK
        assert proven.reason == "No delete files required rewriting."

    def test_disabled_and_skipped_helpers(self):
        disabled = disabled_operation("REWRITE_MANIFESTS")
        skipped = skipped_operation("EXPIRE_SNAPSHOTS", "not_attempted", "Not attempted.")

        assert (disabled.status, disabled.reason) == (OperationStatus.DISABLED, "Disabled by configuration.")
        assert (skipped.status, skipped.reason_code) == (OperationStatus.SKIPPED, "not_attempted")
        assert disabled.started_at is None and skipped.started_at is None


def op(status):
    return OperationResult(operation="X", status=status, reason_code="r", reason="r")


class TestAggregateStatus:
    @pytest.mark.parametrize("statuses, expected", [
        ([OperationStatus.NO_WORK, OperationStatus.DISABLED], OperationStatus.NO_WORK),
        ([OperationStatus.DISABLED, OperationStatus.DISABLED], OperationStatus.DISABLED),
        ([OperationStatus.NO_WORK, OperationStatus.SUCCESS_WITH_WORK], OperationStatus.SUCCESS_WITH_WORK),
        ([OperationStatus.SUCCESS_WITH_WORK, OperationStatus.UNVERIFIED], OperationStatus.UNVERIFIED),
        ([OperationStatus.UNVERIFIED, OperationStatus.PARTIAL], OperationStatus.PARTIAL),
        ([OperationStatus.PARTIAL, OperationStatus.FAILED], OperationStatus.FAILED),
    ])
    def test_table_status_uses_the_most_severe_operation(self, statuses, expected):
        table = TableResult(catalog="c", database="d", table="t", operations=[op(s) for s in statuses])

        assert table.status == expected

    def test_skipped_table(self):
        table = TableResult(catalog="c", database="d", table="t",
                            skip_reason_code="not_iceberg", skip_reason="Table is not an Iceberg table.")

        assert table.status == OperationStatus.SKIPPED

    def test_table_level_error_is_failed(self):
        table = TableResult(catalog="c", database="d", table="t", operations=[op(OperationStatus.NO_WORK)],
                            error_type="RuntimeException", error_message="boom")

        assert table.status == OperationStatus.FAILED

    def test_run_status_ignores_skipped_tables_unless_all_are_skipped(self):
        skipped = TableResult(catalog="c", database="d", table="s", skip_reason_code="lock_held", skip_reason="x")
        no_work = TableResult(catalog="c", database="d", table="n", operations=[op(OperationStatus.NO_WORK)])

        assert RunResult(tables=[skipped, no_work]).status == OperationStatus.NO_WORK
        assert RunResult(tables=[skipped]).status == OperationStatus.SKIPPED
        assert RunResult(tables=[]).status == OperationStatus.NO_WORK
