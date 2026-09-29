"""Tests for the human-readable end-of-run Data Compaction summary."""

from data_compaction_job.compaction_results import (
    OperationResult,
    OperationStatus,
    ReasonCode,
    RunResult,
    SnapshotInfo,
    TableResult,
    TableState,
    disabled_operation,
)
from data_compaction_job.compaction_summary import render_summary
from data_compaction_job.constants import CompactionOperation

DELIMITER = "=" * 80
SEPARATOR = "-" * 80

UNVERIFIED_REASON = (
    "Iceberg returned zero committed rewrite metrics. With partial progress enabled, Iceberg returns the same "
    "zero metrics when no files required rewriting and when every rewrite commit failed, so this result cannot "
    "tell the two apart."
)
WINDOW_NOTE = "Non-replace snapshots observed during the operation window: 2 (append 1, delete 1)."
ZERO_REWRITE = {
    "rewritten_data_files_count": 0,
    "added_data_files_count": 0,
    "rewritten_bytes_count": 0,
    "failed_data_files_count": 0,
}
ZERO_EXPIRE = {
    "deleted_data_files_count": 0,
    "deleted_position_delete_files_count": 0,
    "deleted_equality_delete_files_count": 0,
    "deleted_manifest_files_count": 0,
    "deleted_manifest_lists_count": 0,
    "deleted_statistics_files_count": 0,
}


def rewrite_sql(table):
    return (f"CALL `spark_catalog`.system.rewrite_data_files(table => '`spark_catalog`.`db`.`{table}`', "
            f"options => map('min-input-files', '2'))")


def orders_table():
    return TableResult(
        catalog="spark_catalog", database="db", table="orders",
        before=TableState(snapshot_id=100, data_files=40, delete_files=0, total_files_size=1048576, records=400,
                          snapshots=[SnapshotInfo(100, "append", 1_000)]),
        after=TableState(snapshot_id=101, data_files=2, delete_files=0, total_files_size=1043456, records=400,
                         snapshots=[SnapshotInfo(100, "append", 1_000), SnapshotInfo(101, "replace", 1_010_000)]),
        operations=[
            disabled_operation(CompactionOperation.REWRITE_MANIFESTS),
            OperationResult(
                operation=CompactionOperation.REWRITE_DATA_FILES, status=OperationStatus.SUCCESS,
                reason_code=ReasonCode.COMMITTED,
                reason="Iceberg committed a rewrite of 40 data file(s) into 2 file(s).",
                query=rewrite_sql("orders"), options={"min-input-files": 2},
                started_at=1001.0, ended_at=1013.0,
                metrics={"rewritten_data_files_count": 40, "added_data_files_count": 2,
                         "rewritten_bytes_count": 1048576, "failed_data_files_count": 0}),
            OperationResult(
                operation=CompactionOperation.EXPIRE_SNAPSHOT, status=OperationStatus.NO_WORK,
                reason_code=ReasonCode.NO_SNAPSHOTS_EXPIRED, reason="No snapshots were eligible for expiration.",
                query="CALL expire", started_at=1013.0, ended_at=1014.5, metrics=dict(ZERO_EXPIRE)),
            OperationResult(
                operation=CompactionOperation.REMOVE_ORPHAN_FILES, status=OperationStatus.NO_WORK,
                reason_code=ReasonCode.NO_ORPHAN_FILES, reason="No orphan files were found.",
                query="CALL orphans", started_at=1014.5, ended_at=1017.5, row_count=0),
        ],
    )


def checkpoint_table():
    return TableResult(
        catalog="spark_catalog", database="db", table="checkpoint",
        before=TableState(snapshot_id=500, data_files=12480, delete_files=0, total_files_size=14976000,
                          records=12480, snapshots=[SnapshotInfo(500, "append", 1_000)]),
        after=TableState(snapshot_id=502, data_files=12471, delete_files=0, total_files_size=14965200,
                         records=12471,
                         snapshots=[SnapshotInfo(500, "append", 1_000), SnapshotInfo(501, "append", 1_030_000),
                                    SnapshotInfo(502, "delete", 1_031_000)]),
        operations=[
            disabled_operation(CompactionOperation.REWRITE_MANIFESTS),
            OperationResult(
                operation=CompactionOperation.REWRITE_DATA_FILES, status=OperationStatus.UNVERIFIED,
                reason_code=ReasonCode.ZERO_COMMITTED_UNDER_PARTIAL_PROGRESS, reason=UNVERIFIED_REASON,
                query=rewrite_sql("checkpoint"),
                options={"min-input-files": 2, "partial-progress.enabled": True},
                started_at=1020.0, ended_at=1082.0, metrics=dict(ZERO_REWRITE), notes=[WINDOW_NOTE]),
            disabled_operation(CompactionOperation.EXPIRE_SNAPSHOT),
            disabled_operation(CompactionOperation.REMOVE_ORPHAN_FILES),
        ],
    )


def events_table():
    unchanged = TableState(snapshot_id=7, data_files=3, delete_files=0, total_files_size=2048, records=30,
                           snapshots=[SnapshotInfo(7, "append", 1_000)])
    return TableResult(
        catalog="spark_catalog", database="db", table="events", before=unchanged, after=unchanged,
        operations=[
            OperationResult(operation=CompactionOperation.REWRITE_MANIFESTS, status=OperationStatus.NO_WORK,
                            reason_code=ReasonCode.NO_MANIFESTS_REQUIRED_REWRITING,
                            reason="No manifests required rewriting.", started_at=1000.0, ended_at=1001.0,
                            metrics={"rewritten_manifests_count": 0, "added_manifests_count": 0}),
            OperationResult(operation=CompactionOperation.REWRITE_DATA_FILES, status=OperationStatus.NO_WORK,
                            reason_code=ReasonCode.NO_FILES_REQUIRED_REWRITING, reason="No files required rewriting.",
                            started_at=1001.0, ended_at=1002.0, metrics=dict(ZERO_REWRITE)),
            disabled_operation(CompactionOperation.EXPIRE_SNAPSHOT),
            disabled_operation(CompactionOperation.REMOVE_ORPHAN_FILES),
        ],
    )


def broken_table():
    return TableResult(
        catalog="spark_catalog", database="db", table="broken",
        operations=[
            disabled_operation(CompactionOperation.REWRITE_MANIFESTS),
            OperationResult(
                operation=CompactionOperation.REWRITE_DATA_FILES, status=OperationStatus.FAILED,
                reason_code=ReasonCode.PROCEDURE_ERROR, reason="The Iceberg procedure call failed.",
                query=rewrite_sql("broken"), options={"min-input-files": 2},
                started_at=1090.0, ended_at=1095.0, error_type="ValidationException",
                error_message="Missing required files to delete: s3://bucket/broken/data/f1.parquet"),
            disabled_operation(CompactionOperation.EXPIRE_SNAPSHOT),
            disabled_operation(CompactionOperation.REMOVE_ORPHAN_FILES),
        ],
    )


def skipped_table():
    return TableResult(catalog="spark_catalog", database="db", table="raw_view",
                       skip_reason_code=ReasonCode.NOT_ICEBERG, skip_reason="Table is not an Iceberg table.")


def sample_run():
    return RunResult(
        spark_app_id="app-20260928-0001", catalog="spark_catalog", started_at=1000.0, ended_at=1125.0,
        tables_discovered=5,
        tables=[orders_table(), events_table(), skipped_table(), checkpoint_table(), broken_table()],
    )


EXPECTED_BODY = [
    DELIMITER,
    "DATA COMPACTION SUMMARY",
    DELIMITER,
    "Spark application : app-20260928-0001",
    "Catalog           : spark_catalog",
    "Duration          : 2m 05s",
    "Tables discovered : 5",
    "Tables processed  : 4",
    "Tables skipped    : 1",
    "Table outcomes    : FAILED 1 | UNVERIFIED 1 | SUCCESS 1 | NO_WORK 1 | SKIPPED 1",
    SEPARATOR,
    "TABLE spark_catalog.db.broken",
    "  Status   : FAILED",
    "  Before   : unavailable",
    "  After    : unavailable",
    "  Change   : Table state unavailable; changes cannot be reported.",
    "  Operations:",
    "    REWRITE_MANIFESTS",
    "      Status   : DISABLED",
    "      Reason   : Disabled by configuration.",
    "    REWRITE_DATA_FILES",
    "      Status   : FAILED",
    "      Reason   : The Iceberg procedure call failed.",
    "      Duration : 5.0s",
    "      Options  : min-input-files=2",
    "      Error    : ValidationException: Missing required files to delete: s3://bucket/broken/data/f1.parquet",
    "      SQL      : " + rewrite_sql("broken"),
    "    EXPIRE_SNAPSHOTS",
    "      Status   : DISABLED",
    "      Reason   : Disabled by configuration.",
    "    REMOVE_ORPHAN_FILES",
    "      Status   : DISABLED",
    "      Reason   : Disabled by configuration.",
    SEPARATOR,
    "TABLE spark_catalog.db.checkpoint",
    "  Status   : UNVERIFIED",
    "  Before   : snapshot 500 | data files 12,480 | delete files 0 | total size 14,976,000 bytes | records 12,480",
    "  After    : snapshot 502 | data files 12,471 | delete files 0 | total size 14,965,200 bytes | records 12,471",
    "  Change   : Table state changed from 12,480 to 12,471 data files.",
    "  Change   : Table state changed from 14,976,000 to 14,965,200 bytes (total file size).",
    "  Change   : Table state changed from 12,480 to 12,471 records.",
    "  Note     : Table-state changes are not attributed to individual operations.",
    "  Commits  : 2 new snapshot(s) during maintenance: append 1, delete 1",
    "  Operations:",
    "    REWRITE_MANIFESTS",
    "      Status   : DISABLED",
    "      Reason   : Disabled by configuration.",
    "    REWRITE_DATA_FILES",
    "      Status   : UNVERIFIED",
    "      Reason   : " + UNVERIFIED_REASON,
    "      Duration : 1m 02s",
    "      Iceberg  : rewritten_data_files_count=0, added_data_files_count=0, rewritten_bytes_count=0, "
    "failed_data_files_count=0",
    "      Options  : min-input-files=2, partial-progress.enabled=true",
    "      Note     : " + WINDOW_NOTE,
    "      SQL      : " + rewrite_sql("checkpoint"),
    "    EXPIRE_SNAPSHOTS",
    "      Status   : DISABLED",
    "      Reason   : Disabled by configuration.",
    "    REMOVE_ORPHAN_FILES",
    "      Status   : DISABLED",
    "      Reason   : Disabled by configuration.",
    SEPARATOR,
    "TABLE spark_catalog.db.orders",
    "  Status   : SUCCESS",
    "  Before   : snapshot 100 | data files 40 | delete files 0 | total size 1,048,576 bytes | records 400",
    "  After    : snapshot 101 | data files 2 | delete files 0 | total size 1,043,456 bytes | records 400",
    "  Change   : Table state changed from 40 to 2 data files.",
    "  Change   : Table state changed from 1,048,576 to 1,043,456 bytes (total file size).",
    "  Note     : Table-state changes are not attributed to individual operations.",
    "  Commits  : 1 new snapshot(s) during maintenance: replace 1",
    "  Operations:",
    "    REWRITE_MANIFESTS",
    "      Status   : DISABLED",
    "      Reason   : Disabled by configuration.",
    "    REWRITE_DATA_FILES",
    "      Status   : SUCCESS",
    "      Reason   : Iceberg committed a rewrite of 40 data file(s) into 2 file(s).",
    "      Duration : 12.0s",
    "      Iceberg  : rewritten_data_files_count=40, added_data_files_count=2, rewritten_bytes_count=1048576, "
    "failed_data_files_count=0",
    "      Options  : min-input-files=2",
    "      SQL      : " + rewrite_sql("orders"),
    "    EXPIRE_SNAPSHOTS",
    "      Status   : NO_WORK",
    "      Reason   : No snapshots were eligible for expiration.",
    "      Duration : 1.5s",
    "    REMOVE_ORPHAN_FILES",
    "      Status   : NO_WORK",
    "      Reason   : No orphan files were found.",
    "      Duration : 3.0s",
    SEPARATOR,
    "NO WORK OR DISABLED (1 table(s))",
    "  spark_catalog.db.events  NO_WORK",
    SEPARATOR,
    "SKIPPED (1 table(s))",
    "  spark_catalog.db.raw_view  not_iceberg: Table is not an Iceberg table.",
    SEPARATOR,
    "FAILURES (1)",
    "  spark_catalog.db.broken  REWRITE_DATA_FILES  "
    "ValidationException: Missing required files to delete: s3://bucket/broken/data/f1.parquet",
    SEPARATOR,
    "NEEDS REVIEW (1)",
    "  spark_catalog.db.checkpoint  REWRITE_DATA_FILES  " + UNVERIFIED_REASON,
    DELIMITER,
    "OVERALL STATUS: FAILED",
    DELIMITER,
]


def test_renders_the_expected_summary_exactly():
    expected = "\n" * 7 + "\n".join(EXPECTED_BODY) + "\n" * 7

    assert render_summary(sample_run()) == expected


def test_enum_members_are_rendered_as_plain_names_and_values():
    text = render_summary(sample_run())

    assert "CompactionOperation." not in text
    assert "OperationStatus." not in text
    assert "ReasonCode." not in text


def test_report_is_framed_by_seven_blank_lines_and_delimiters():
    lines = render_summary(sample_run()).split("\n")

    assert lines[:7] == [""] * 7
    assert lines[7] == DELIMITER
    assert lines[8] == "DATA COMPACTION SUMMARY"
    assert lines[-8] == DELIMITER
    assert lines[-9] == "OVERALL STATUS: FAILED"
    assert lines[-7:] == [""] * 7


def test_table_outcomes_follow_the_severity_order():
    disabled = TableResult(catalog="c", database="d", table="a",
                           operations=[disabled_operation(operation) for operation in CompactionOperation])
    skipped = TableResult(catalog="c", database="d", table="b", skip_reason_code=ReasonCode.LOCK_HELD,
                          skip_reason="The compaction lock was not acquired.")
    run = RunResult(spark_app_id="app", catalog="c", started_at=0.0, ended_at=1.0, tables_discovered=2,
                    tables=[skipped, disabled])

    assert "Table outcomes    : DISABLED 1 | SKIPPED 1" in render_summary(run).split("\n")


def test_no_work_tables_take_one_line():
    run = RunResult(spark_app_id="app", catalog="spark_catalog", started_at=0.0, ended_at=1.0,
                    tables_discovered=1, tables=[events_table()])

    text = render_summary(run)

    assert "  spark_catalog.db.events  NO_WORK" in text
    assert "TABLE spark_catalog.db.events" not in text
    assert "OVERALL STATUS: NO_WORK" in text


def test_no_work_operation_in_a_detailed_table_is_stated_plainly():
    table = TableResult(catalog="c", database="d", table="t", operations=[
        OperationResult(operation=CompactionOperation.REWRITE_DATA_FILES, status=OperationStatus.NO_WORK,
                        reason_code=ReasonCode.NO_FILES_REQUIRED_REWRITING, reason="No files required rewriting.",
                        started_at=0.0, ended_at=1.0, metrics=dict(ZERO_REWRITE)),
        OperationResult(operation=CompactionOperation.REMOVE_ORPHAN_FILES, status=OperationStatus.FAILED,
                        reason_code=ReasonCode.PROCEDURE_ERROR, reason="The Iceberg procedure call failed.",
                        started_at=1.0, ended_at=2.0, error_type="IOException", error_message="access denied"),
    ])
    run = RunResult(spark_app_id="app", catalog="c", started_at=0.0, ended_at=2.0, tables_discovered=1,
                    tables=[table])

    lines = render_summary(run).split("\n")
    start = lines.index("    REWRITE_DATA_FILES")

    assert lines[start + 1] == "      Status   : NO_WORK"
    assert lines[start + 2] == "      Reason   : No files required rewriting."


def test_unavailable_state_fields_are_never_rendered_as_zero():
    partial_state = TableState(snapshot_id=9, data_files=4)
    table = TableResult(catalog="c", database="d", table="t", before=partial_state, after=partial_state,
                        operations=[OperationResult(operation=CompactionOperation.REWRITE_DATA_FILES,
                                                    status=OperationStatus.UNVERIFIED,
                                                    reason_code=ReasonCode.ZERO_COMMITTED_UNDER_PARTIAL_PROGRESS,
                                                    reason="x", started_at=0.0, ended_at=1.0)])
    run = RunResult(spark_app_id="app", catalog="c", started_at=0.0, ended_at=1.0, tables_discovered=1,
                    tables=[table])

    text = render_summary(run)

    assert ("  Before   : snapshot 9 | data files 4 | delete files unavailable | total size unavailable | "
            "records unavailable") in text


def test_state_changes_are_never_attributed_to_compaction():
    text = render_summary(sample_run()).lower()

    assert "reduced" not in text
    assert "compaction changed" not in text


def detailed_run(*operations):
    failing = OperationResult(operation=CompactionOperation.REMOVE_ORPHAN_FILES, status=OperationStatus.FAILED,
                              reason_code=ReasonCode.PROCEDURE_ERROR, reason="The Iceberg procedure call failed.",
                              started_at=1.0, ended_at=2.0, error_type="IOException", error_message="access denied")
    table = TableResult(catalog="c", database="d", table="t", operations=[*operations, failing])
    return RunResult(spark_app_id="app", catalog="c", started_at=0.0, ended_at=2.0, tables_discovered=1,
                     tables=[table])


def operation_block(text, operation):
    lines = text.split("\n")
    start = lines.index(f"    {operation}")
    end = next(i for i in range(start + 1, len(lines)) if not lines[i].startswith("      "))
    return lines[start:end]


def test_no_work_with_all_zero_metrics_omits_the_zero_counter_line():
    operation = OperationResult(operation=CompactionOperation.REWRITE_DATA_FILES, status=OperationStatus.NO_WORK,
                                reason_code=ReasonCode.NO_FILES_REQUIRED_REWRITING,
                                reason="No files required rewriting.", started_at=0.0, ended_at=1.0,
                                metrics=dict(ZERO_REWRITE))

    block = operation_block(render_summary(detailed_run(operation)), "REWRITE_DATA_FILES")

    assert block == [
        "    REWRITE_DATA_FILES",
        "      Status   : NO_WORK",
        "      Reason   : No files required rewriting.",
        "      Duration : 1.0s",
    ]
    assert operation.metrics == ZERO_REWRITE


def test_no_work_with_zero_returned_rows_omits_the_row_count_line():
    operation = OperationResult(operation=CompactionOperation.REMOVE_ORPHAN_FILES, status=OperationStatus.NO_WORK,
                                reason_code=ReasonCode.NO_ORPHAN_FILES, reason="No orphan files were found.",
                                started_at=0.0, ended_at=1.0, row_count=0)
    table = TableResult(catalog="c", database="d", table="t", operations=[operation, OperationResult(
        operation=CompactionOperation.REWRITE_DATA_FILES, status=OperationStatus.FAILED,
        reason_code=ReasonCode.PROCEDURE_ERROR, reason="The Iceberg procedure call failed.",
        started_at=1.0, ended_at=2.0, error_type="E", error_message="m")])
    run = RunResult(spark_app_id="app", catalog="c", started_at=0.0, ended_at=2.0, tables_discovered=1,
                    tables=[table])

    block = operation_block(render_summary(run), "REMOVE_ORPHAN_FILES")

    assert not any(line.strip().startswith("Iceberg") for line in block)


def test_no_work_with_non_zero_evidence_keeps_the_metrics_line():
    operation = OperationResult(operation=CompactionOperation.REWRITE_DATA_FILES, status=OperationStatus.NO_WORK,
                                reason_code=ReasonCode.NO_FILES_REQUIRED_REWRITING,
                                reason="No files required rewriting.", started_at=0.0, ended_at=1.0,
                                metrics={"rewritten_data_files_count": 0, "other": 3})

    block = operation_block(render_summary(detailed_run(operation)), "REWRITE_DATA_FILES")

    assert "      Iceberg  : rewritten_data_files_count=0, other=3" in block


def test_zero_metrics_are_still_shown_for_non_no_work_statuses():
    operation = OperationResult(operation=CompactionOperation.REWRITE_DATA_FILES, status=OperationStatus.UNVERIFIED,
                                reason_code=ReasonCode.ZERO_COMMITTED_UNDER_PARTIAL_PROGRESS,
                                reason=UNVERIFIED_REASON, started_at=0.0, ended_at=1.0, metrics=dict(ZERO_REWRITE))

    block = operation_block(render_summary(detailed_run(operation)), "REWRITE_DATA_FILES")

    assert any(line.startswith("      Iceberg  : rewritten_data_files_count=0") for line in block)


def test_needs_review_lists_every_unverified_operation_after_failures():
    lines = render_summary(sample_run()).split("\n")

    failures, review = lines.index("FAILURES (1)"), lines.index("NEEDS REVIEW (1)")
    assert failures < review
    assert lines[review + 1] == "  spark_catalog.db.checkpoint  REWRITE_DATA_FILES  " + UNVERIFIED_REASON
    assert lines[review + 2] == DELIMITER


def test_needs_review_is_omitted_when_nothing_is_unverified():
    run = RunResult(spark_app_id="app", catalog="spark_catalog", started_at=0.0, ended_at=1.0,
                    tables_discovered=2, tables=[orders_table(), broken_table()])

    assert "NEEDS REVIEW" not in render_summary(run)
