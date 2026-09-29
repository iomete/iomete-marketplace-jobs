from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class OperationStatus(str, Enum):
    SUCCESS_WITH_WORK = "SUCCESS_WITH_WORK"
    NO_WORK = "NO_WORK"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    DISABLED = "DISABLED"
    UNVERIFIED = "UNVERIFIED"


SEVERITY = [
    OperationStatus.FAILED,
    OperationStatus.PARTIAL,
    OperationStatus.UNVERIFIED,
    OperationStatus.SUCCESS_WITH_WORK,
    OperationStatus.NO_WORK,
    OperationStatus.SKIPPED,
    OperationStatus.DISABLED,
]


@dataclass
class SnapshotInfo:
    snapshot_id: int
    operation: Optional[str]
    committed_at_ms: Optional[int]


@dataclass
class TableState:
    snapshot_id: Optional[int] = None
    data_files: Optional[int] = None
    delete_files: Optional[int] = None
    total_files_size: Optional[int] = None
    records: Optional[int] = None
    position_deletes: Optional[int] = None
    equality_deletes: Optional[int] = None
    snapshots: Optional[list[SnapshotInfo]] = None


@dataclass
class ProcedureOutcome:
    query: Optional[str]
    rows: Optional[list]
    error: Optional[BaseException]
    started_at: float
    ended_at: float


@dataclass
class OperationResult:
    operation: str
    status: OperationStatus
    reason_code: str
    reason: str
    query: Optional[str] = None
    options: Optional[dict] = None
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    metrics: Optional[dict] = None
    row_count: Optional[int] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    notes: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at is None or self.ended_at is None:
            return None
        return self.ended_at - self.started_at


@dataclass
class TableResult:
    catalog: str
    database: str
    table: str
    before: Optional[TableState] = None
    after: Optional[TableState] = None
    operations: list[OperationResult] = field(default_factory=list)
    skip_reason_code: Optional[str] = None
    skip_reason: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def name(self) -> str:
        return f"{self.catalog}.{self.database}.{self.table}"

    @property
    def status(self) -> OperationStatus:
        if self.skip_reason_code:
            return OperationStatus.SKIPPED
        if self.error_type or self.error_message:
            return OperationStatus.FAILED
        return _most_severe([op.status for op in self.operations], OperationStatus.NO_WORK)


@dataclass
class RunResult:
    spark_app_id: Optional[str] = None
    catalog: Optional[str] = None
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    tables_discovered: int = 0
    tables: list[TableResult] = field(default_factory=list)

    @property
    def status(self) -> OperationStatus:
        processed = [table.status for table in self.tables if table.status != OperationStatus.SKIPPED]
        if processed:
            return _most_severe(processed, OperationStatus.NO_WORK)
        return OperationStatus.SKIPPED if self.tables else OperationStatus.NO_WORK


def _most_severe(statuses, default):
    for status in SEVERITY:
        if status in statuses:
            return status
    return default


def partial_progress_enabled(options: Optional[dict]) -> bool:
    if not options:
        return False
    return str(options.get("partial-progress.enabled", "false")).lower() == "true"


def partial_progress_hides_commit_failures(options: Optional[dict]) -> bool:
    if not partial_progress_enabled(options):
        return False
    max_failed = options.get("partial-progress.max-failed-commits")
    try:
        # Iceberg throws once failed commits exceed this limit, so a limit of 0 means no failure is tolerated.
        return max_failed is None or int(str(max_failed)) > 0
    except ValueError:
        return True


@dataclass(frozen=True)
class OperationSpec:
    name: str
    work_counters: tuple
    no_work_reason: str
    commit_failures_hidden: Callable[[Optional[dict]], bool] = lambda options: False
    no_work_reason_code: str = "no_work"
    committed_counter: Optional[str] = None
    failure_counter: Optional[str] = None
    returns_file_rows: bool = False
    zero_requires_snapshot_evidence: bool = False
    describe_work: Optional[Callable[[dict], str]] = None


def _count(metrics: dict, key: str) -> int:
    value = metrics.get(key)
    return int(value) if value is not None else 0


def _describe_rewrite(metrics: dict) -> str:
    parts = []
    rewritten, added = _count(metrics, "rewritten_data_files_count"), _count(metrics, "added_data_files_count")
    if rewritten or added:
        parts.append(f"Iceberg committed a rewrite of {rewritten} data file(s) into {added} file(s).")
    removed = _count(metrics, "removed_delete_files_count")
    if removed:
        parts.append(f"Iceberg removed {removed} dangling delete file(s).")
    return " ".join(parts) or _describe_counters(metrics)


def _describe_counters(metrics: dict) -> str:
    return "Iceberg reported: " + ", ".join(f"{key}={value}" for key, value in metrics.items()) + "."


EXPIRE_COUNTERS = (
    "deleted_data_files_count",
    "deleted_position_delete_files_count",
    "deleted_equality_delete_files_count",
    "deleted_manifest_files_count",
    "deleted_manifest_lists_count",
    "deleted_statistics_files_count",
)

OPERATION_SPECS = {
    spec.name: spec for spec in (
        OperationSpec(
            name="REWRITE_MANIFESTS",
            work_counters=("rewritten_manifests_count", "added_manifests_count"),
            no_work_reason="No manifests required rewriting.",
            no_work_reason_code="no_manifests_required_rewriting",
            describe_work=lambda m: (f"Iceberg rewrote {_count(m, 'rewritten_manifests_count')} manifest(s) "
                                     f"into {_count(m, 'added_manifests_count')} manifest(s)."),
        ),
        OperationSpec(
            name="REWRITE_DATA_FILES",
            work_counters=("rewritten_data_files_count", "added_data_files_count", "rewritten_bytes_count",
                           "removed_delete_files_count"),
            no_work_reason="No files required rewriting.",
            no_work_reason_code="no_files_required_rewriting",
            commit_failures_hidden=partial_progress_hides_commit_failures,
            committed_counter="rewritten_data_files_count",
            failure_counter="failed_data_files_count",
            describe_work=_describe_rewrite,
        ),
        OperationSpec(
            name="EXPIRE_SNAPSHOTS",
            work_counters=EXPIRE_COUNTERS,
            no_work_reason="No snapshots were eligible for expiration.",
            no_work_reason_code="no_snapshots_expired",
            zero_requires_snapshot_evidence=True,
            describe_work=lambda m: (f"Iceberg deleted {sum(_count(m, key) for key in EXPIRE_COUNTERS)} file(s) "
                                     f"belonging to expired snapshots."),
        ),
        OperationSpec(
            name="REMOVE_ORPHAN_FILES",
            work_counters=(),
            no_work_reason="No orphan files were found.",
            no_work_reason_code="no_orphan_files",
            returns_file_rows=True,
        ),
    )
}

ZERO_UNDER_PARTIAL_PROGRESS = (
    "Iceberg returned zero committed rewrite metrics. With partial progress enabled, Iceberg returns the same "
    "zero metrics when no files required rewriting and when every rewrite commit failed, so this result cannot "
    "tell the two apart."
)
PARTIAL_PROGRESS_NOTE = (
    "Partial progress is enabled: Iceberg counts only committed rewrite groups and does not return failed "
    "commits, so these counts may not include every planned group."
)


def disabled_operation(operation: str) -> OperationResult:
    return OperationResult(operation=operation, status=OperationStatus.DISABLED, reason_code="disabled",
                           reason="Disabled by configuration.")


def skipped_operation(operation: str, reason_code: str, reason: str) -> OperationResult:
    return OperationResult(operation=operation, status=OperationStatus.SKIPPED, reason_code=reason_code,
                           reason=reason)


def unreadable_operation(operation: str, error: BaseException) -> OperationResult:
    return OperationResult(operation=operation, status=OperationStatus.FAILED, reason_code="result_unreadable",
                           reason="The Iceberg procedure result could not be read.",
                           error_type=type(error).__name__, error_message=str(error))


def classify_operation(operation: str, outcome: ProcedureOutcome, options: Optional[dict] = None,
                       before: Optional[TableState] = None, after: Optional[TableState] = None,
                       spec: Optional[OperationSpec] = None) -> OperationResult:
    spec = spec or OPERATION_SPECS.get(operation)
    result = OperationResult(operation=operation, status=OperationStatus.UNVERIFIED, reason_code="", reason="",
                             query=outcome.query, options=options, started_at=outcome.started_at,
                             ended_at=outcome.ended_at)

    if outcome.error is not None:
        return _set(result, OperationStatus.FAILED, "procedure_error", "Iceberg procedure failed.",
                    error_type=type(outcome.error).__name__, error_message=str(outcome.error))

    rows = outcome.rows or []
    if spec is not None and spec.returns_file_rows:
        result.row_count = len(rows)
        if rows:
            return _set(result, OperationStatus.SUCCESS_WITH_WORK, "files_returned",
                        f"Iceberg returned {len(rows)} orphan file location(s).")
        return _set(result, OperationStatus.NO_WORK, spec.no_work_reason_code, spec.no_work_reason)

    if not rows:
        return _set(result, OperationStatus.FAILED, "result_unreadable", "Iceberg returned no result row.")
    result.metrics = rows[0].asDict()

    if spec is None:
        return _classify_unknown(result)

    failed = _count(result.metrics, spec.failure_counter) if spec.failure_counter else 0
    committed = _count(result.metrics, spec.committed_counter) if spec.committed_counter else 0
    has_work = any(_count(result.metrics, key) > 0 for key in spec.work_counters)
    hidden = spec.commit_failures_hidden(options)

    if failed and committed:
        return _set(result, OperationStatus.PARTIAL, "some_groups_failed",
                    f"Iceberg committed {committed} rewritten data file(s) and reported {failed} data file(s) "
                    f"in failed rewrite groups.")
    if failed:
        return _set(result, OperationStatus.FAILED, "rewrite_groups_failed",
                    f"Iceberg reported {failed} data file(s) in failed rewrite groups and committed none.")
    if has_work:
        describe = spec.describe_work or _describe_counters
        _set(result, OperationStatus.SUCCESS_WITH_WORK, "committed", describe(result.metrics))
        if hidden:
            result.notes.append(PARTIAL_PROGRESS_NOTE)
        return result
    if spec.zero_requires_snapshot_evidence:
        return _classify_zero_expiration(result, spec, before, after)
    if hidden:
        _set(result, OperationStatus.UNVERIFIED, "zero_committed_under_partial_progress", ZERO_UNDER_PARTIAL_PROGRESS)
        result.notes.extend(_window_notes(outcome, after))
        return result
    return _set(result, OperationStatus.NO_WORK, spec.no_work_reason_code, spec.no_work_reason)


def _set(result: OperationResult, status: OperationStatus, reason_code: str, reason: str,
         **fields: Any) -> OperationResult:
    result.status, result.reason_code, result.reason = status, reason_code, reason
    for name, value in fields.items():
        setattr(result, name, value)
    return result


def _classify_unknown(result: OperationResult) -> OperationResult:
    counters = [value for value in result.metrics.values()
                if isinstance(value, (int, float)) and not isinstance(value, bool)]
    if any(value > 0 for value in counters):
        return _set(result, OperationStatus.SUCCESS_WITH_WORK, "committed", _describe_counters(result.metrics))
    return _set(result, OperationStatus.UNVERIFIED, "unknown_operation_semantics",
                "Iceberg returned zero counters for an operation whose result semantics are not known to this job.")


def _classify_zero_expiration(result, spec, before, after) -> OperationResult:
    if before is None or after is None or before.snapshots is None or after.snapshots is None:
        return _set(result, OperationStatus.UNVERIFIED, "table_state_unavailable",
                    "Iceberg reported no deleted files, and table state was not available to confirm that no "
                    "snapshots expired.")
    remaining = {snapshot.snapshot_id for snapshot in after.snapshots}
    removed = [snapshot for snapshot in before.snapshots if snapshot.snapshot_id not in remaining]
    if removed:
        return _set(result, OperationStatus.UNVERIFIED, "snapshots_removed_without_deleted_files",
                    f"Iceberg reported no deleted files, but {len(removed)} snapshot(s) present before maintenance "
                    f"were absent afterwards; attribution unavailable.")
    return _set(result, OperationStatus.NO_WORK, spec.no_work_reason_code, spec.no_work_reason)


def _window_notes(outcome: ProcedureOutcome, after: Optional[TableState]) -> list[str]:
    if after is None or after.snapshots is None:
        return []
    start_ms, end_ms = outcome.started_at * 1000, outcome.ended_at * 1000
    in_window = [snapshot for snapshot in after.snapshots
                 if snapshot.committed_at_ms is not None and start_ms <= snapshot.committed_at_ms <= end_ms]
    notes = []
    non_replace = [snapshot for snapshot in in_window if snapshot.operation != "replace"]
    if non_replace:
        notes.append(f"Non-replace snapshots observed during the operation window: {len(non_replace)} "
                     f"({operation_counts(non_replace)}).")
    replace_count = len(in_window) - len(non_replace)
    if replace_count:
        notes.append(f"Replace snapshots observed during the operation window: {replace_count}; "
                     f"attribution unavailable.")
    return notes


def operation_counts(snapshots: list[SnapshotInfo]) -> str:
    counts = Counter(snapshot.operation or "unknown" for snapshot in snapshots)
    return ", ".join(f"{operation} {count}" for operation, count in sorted(counts.items()))
