from collections import Counter
from typing import Any, Optional

from data_compaction_job.compaction_results import (
    SEVERITY,
    OperationResult,
    OperationStatus,
    RunResult,
    TableResult,
    TableState,
    operation_counts,
)

DELIMITER = "=" * 80
SEPARATOR = "-" * 80
DETAILED_STATUSES = {OperationStatus.FAILED, OperationStatus.PARTIAL, OperationStatus.UNVERIFIED,
                     OperationStatus.SUCCESS}
BLANK_LINES = "\n" * 7


def render_summary(run: RunResult) -> str:
    tables = sorted(run.tables, key=lambda table: table.name)
    skipped = [table for table in tables if table.status == OperationStatus.SKIPPED]
    processed = [table for table in tables if table.status != OperationStatus.SKIPPED]
    detailed = [table for table in processed if table.status in DETAILED_STATUSES]
    quiet = [table for table in processed if table.status not in DETAILED_STATUSES]

    outcome_counts = Counter(table.status for table in tables)
    outcomes = " | ".join(f"{status.value} {outcome_counts[status]}" for status in SEVERITY
                          if outcome_counts[status])

    lines = [
        DELIMITER,
        "DATA COMPACTION SUMMARY",
        DELIMITER,
        _field("Spark application", run.spark_app_id or "unavailable", 17),
        _field("Catalog", run.catalog or "unavailable", 17),
        _field("Duration", _duration(run.started_at, run.ended_at), 17),
        _field("Tables discovered", str(run.tables_discovered), 17),
        _field("Tables processed", str(len(processed)), 17),
        _field("Tables skipped", str(len(skipped)), 17),
        _field("Table outcomes", outcomes or "none", 17),
    ]
    for table in detailed:
        lines += [SEPARATOR] + _table_lines(table)
    if quiet:
        lines += [SEPARATOR, f"NO WORK OR DISABLED ({len(quiet)} table(s))"]
        lines += [f"  {table.name}  {table.status.value}" for table in quiet]
    if skipped:
        lines += [SEPARATOR, f"SKIPPED ({len(skipped)} table(s))"]
        lines += [f"  {table.name}  {table.skip_reason_code.value}: {table.skip_reason}" for table in skipped]
    failures = _failure_lines(tables)
    if failures:
        lines += [SEPARATOR, f"FAILURES ({len(failures)})"] + failures
    review = _review_lines(tables)
    if review:
        lines += [SEPARATOR, f"NEEDS REVIEW ({len(review)})"] + review
    lines += [DELIMITER, f"OVERALL STATUS: {run.status.value}", DELIMITER]
    return BLANK_LINES + "\n".join(lines) + BLANK_LINES


def _field(label: str, value: str, width: int, indent: str = "") -> str:
    return f"{indent}{label:<{width}} : {value}"


def _duration(started_at: Optional[float], ended_at: Optional[float]) -> str:
    if started_at is None or ended_at is None:
        return "unavailable"
    seconds = ended_at - started_at
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, remainder = divmod(int(seconds), 60)
    return f"{minutes}m {remainder:02d}s"


def _number(value: Optional[int]) -> str:
    return "unavailable" if value is None else f"{value:,}"


def _state(state: Optional[TableState]) -> str:
    if state is None:
        return "unavailable"
    snapshot = "unavailable" if state.snapshot_id is None else str(state.snapshot_id)
    size = "unavailable" if state.total_files_size is None else f"{state.total_files_size:,} bytes"
    return (f"snapshot {snapshot} | data files {_number(state.data_files)} | "
            f"delete files {_number(state.delete_files)} | total size {size} | records {_number(state.records)}")


STATE_CHANGES = (
    ("data_files", "data files"),
    ("delete_files", "delete files"),
    ("total_files_size", "bytes (total file size)"),
    ("records", "records"),
)


def _table_lines(table: TableResult) -> list[str]:
    lines = [f"TABLE {table.name}", _field("Status", table.status.value, 8, "  ")]
    if table.error_type or table.error_message:
        lines.append(_field("Error", _error(table.error_type, table.error_message), 8, "  "))
    lines.append(_field("Before", _state(table.before), 8, "  "))
    lines.append(_field("After", _state(table.after), 8, "  "))
    lines += _change_lines(table.before, table.after)
    if table.operations:
        lines.append("  Operations:")
        for operation in table.operations:
            lines += _operation_lines(operation)
    else:
        lines.append("  Operations: none attempted")
    return lines


def _change_lines(before: Optional[TableState], after: Optional[TableState]) -> list[str]:
    if before is None or after is None:
        return [_field("Change", "Table state unavailable; changes cannot be reported.", 8, "  ")]
    changes = []
    for attribute, label in STATE_CHANGES:
        old, new = getattr(before, attribute), getattr(after, attribute)
        if old is not None and new is not None and old != new:
            changes.append(_field("Change", f"Table state changed from {old:,} to {new:,} {label}.", 8, "  "))
    if changes:
        changes.append(_field("Note", "Table-state changes may include concurrent writes and are not attributed to individual maintenance operations.", 8, "  "))
    else:
        changes.append(_field("Change", "No table state change was observed.", 8, "  "))
    if before.snapshots is not None and after.snapshots is not None:
        known = {snapshot.snapshot_id for snapshot in before.snapshots}
        new_snapshots = [snapshot for snapshot in after.snapshots if snapshot.snapshot_id not in known]
        if new_snapshots:
            commits = (f"{len(new_snapshots)} new snapshot(s) during maintenance: "
                       f"{operation_counts(new_snapshots)}")
        else:
            commits = "No new snapshots during maintenance."
        changes.append(_field("Commits", commits, 8, "  "))
    return changes


def _operation_lines(operation: OperationResult) -> list[str]:
    indent = "      "
    lines = [f"    {operation.name}",
             _field("Status", operation.status.value, 8, indent),
             _field("Reason", operation.reason, 8, indent)]
    if operation.started_at is not None:
        lines.append(_field("Duration", _duration(operation.started_at, operation.ended_at), 8, indent))
    if operation.status == OperationStatus.NO_WORK and _only_zero_evidence(operation):
        return lines
    if operation.metrics is not None:
        metrics = ", ".join(f"{key}={value}" for key, value in operation.metrics.items())
        lines.append(_field("Iceberg", metrics, 8, indent))
    elif operation.row_count is not None:
        lines.append(_field("Iceberg", f"{operation.row_count} row(s) returned", 8, indent))
    if operation.status == OperationStatus.NO_WORK:
        return lines
    if operation.options:
        options = ", ".join(f"{key}={_option_value(value)}" for key, value in operation.options.items())
        lines.append(_field("Options", options, 8, indent))
    if operation.error_type or operation.error_message:
        lines.append(_field("Error", _error(operation.error_type, operation.error_message), 8, indent))
    lines += [_field("Note", note, 8, indent) for note in operation.notes]
    if operation.query:
        lines.append(_field("SQL", operation.query, 8, indent))
    return lines


def _only_zero_evidence(operation: OperationResult) -> bool:
    if operation.metrics is not None:
        return all(isinstance(value, (int, float)) and not isinstance(value, bool) and value == 0
                   for value in operation.metrics.values())
    return not operation.row_count


def _option_value(value: Any) -> str:
    return str(value).lower() if isinstance(value, bool) else str(value)


def _error(error_type: Optional[str], message: Optional[str]) -> str:
    return f"{error_type}: {message}" if error_type else (message or "")


def _failure_lines(tables: list[TableResult]) -> list[str]:
    lines = []
    for table in tables:
        if table.error_type or table.error_message:
            lines.append(f"  {table.name}  TABLE  {_error(table.error_type, table.error_message)}")
        for operation in table.operations:
            if operation.status == OperationStatus.FAILED:
                detail = _error(operation.error_type, operation.error_message) or operation.reason
                lines.append(f"  {table.name}  {operation.name}  {detail}")
    return lines


def _review_lines(tables: list[TableResult]) -> list[str]:
    return [f"  {table.name}  {operation.name}  {operation.reason}"
            for table in tables for operation in table.operations
            if operation.status == OperationStatus.UNVERIFIED]
