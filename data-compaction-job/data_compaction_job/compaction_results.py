from collections import Counter
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from data_compaction_job.constants import CompactionOperation


class OperationStatus(str, Enum):
    SUCCESS = "SUCCESS"
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
    OperationStatus.SUCCESS,
    OperationStatus.NO_WORK,
    OperationStatus.DISABLED,
    OperationStatus.SKIPPED,
]


class ReasonCode(str, Enum):
    NOT_ICEBERG = "not_iceberg"
    LOCK_HELD = "lock_held"
    DISABLED = "disabled"
    NOT_ATTEMPTED = "not_attempted"
    FAILED_BEFORE_PROCEDURE_CALL = "failed_before_procedure_call"
    FAILED_OUTSIDE_PROCEDURE_CALL = "failed_outside_procedure_call"
    PROCEDURE_ERROR = "procedure_error"
    RESULT_UNREADABLE = "result_unreadable"
    FILES_RETURNED = "files_returned"
    COMMITTED = "committed"
    SOME_GROUPS_FAILED = "some_groups_failed"
    REWRITE_GROUPS_FAILED = "rewrite_groups_failed"
    ZERO_COMMITTED_UNDER_PARTIAL_PROGRESS = "zero_committed_under_partial_progress"
    TABLE_STATE_UNAVAILABLE = "table_state_unavailable"
    SNAPSHOTS_REMOVED_WITHOUT_DELETED_FILES = "snapshots_removed_without_deleted_files"
    NO_MANIFESTS_REQUIRED_REWRITING = "no_manifests_required_rewriting"
    NO_FILES_REQUIRED_REWRITING = "no_files_required_rewriting"
    NO_SNAPSHOTS_EXPIRED = "no_snapshots_expired"
    NO_ORPHAN_FILES = "no_orphan_files"


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
    snapshots: Optional[list[SnapshotInfo]] = None


@dataclass
class ProcedureOutcome:
    query: Optional[str]
    rows: Optional[list]
    error: Optional[BaseException]
    started_at: float
    ended_at: float


PROCEDURE_QUERY_ATTRIBUTE = "compaction_query"


@contextmanager
def attach_query_to_errors(query: str):
    try:
        yield
    except Exception as e:
        with suppress(Exception):
            setattr(e, PROCEDURE_QUERY_ATTRIBUTE, query)
        raise


@dataclass
class OperationResult:
    operation: CompactionOperation
    status: OperationStatus
    reason_code: ReasonCode
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
    def name(self) -> str:
        return OPERATION_SPECS[self.operation].name


@dataclass
class TableResult:
    catalog: str
    database: str
    table: str
    before: Optional[TableState] = None
    after: Optional[TableState] = None
    operations: list[OperationResult] = field(default_factory=list)
    skip_reason_code: Optional[ReasonCode] = None
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
    operation: CompactionOperation
    name: str
    work_counters: tuple
    no_work_reason: str
    no_work_reason_code: ReasonCode
    commit_failures_hidden: Callable[[Optional[dict]], bool] = lambda options: False
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
    spec.operation: spec for spec in (
        OperationSpec(
            operation=CompactionOperation.REWRITE_MANIFESTS,
            name="REWRITE_MANIFESTS",
            work_counters=("rewritten_manifests_count", "added_manifests_count"),
            no_work_reason="No manifests required rewriting.",
            no_work_reason_code=ReasonCode.NO_MANIFESTS_REQUIRED_REWRITING,
            describe_work=lambda m: (f"Iceberg rewrote {_count(m, 'rewritten_manifests_count')} manifest(s) "
                                     f"into {_count(m, 'added_manifests_count')} manifest(s)."),
        ),
        OperationSpec(
            operation=CompactionOperation.REWRITE_DATA_FILES,
            name="REWRITE_DATA_FILES",
            work_counters=("rewritten_data_files_count", "added_data_files_count", "rewritten_bytes_count",
                           "removed_delete_files_count"),
            no_work_reason="No files required rewriting.",
            no_work_reason_code=ReasonCode.NO_FILES_REQUIRED_REWRITING,
            commit_failures_hidden=partial_progress_hides_commit_failures,
            committed_counter="rewritten_data_files_count",
            failure_counter="failed_data_files_count",
            describe_work=_describe_rewrite,
        ),
        OperationSpec(
            operation=CompactionOperation.EXPIRE_SNAPSHOT,
            name="EXPIRE_SNAPSHOTS",
            work_counters=EXPIRE_COUNTERS,
            no_work_reason="No snapshots were eligible for expiration.",
            no_work_reason_code=ReasonCode.NO_SNAPSHOTS_EXPIRED,
            zero_requires_snapshot_evidence=True,
            describe_work=lambda m: (f"Iceberg deleted {sum(_count(m, key) for key in EXPIRE_COUNTERS)} file(s) "
                                     f"belonging to expired snapshots."),
        ),
        OperationSpec(
            operation=CompactionOperation.REMOVE_ORPHAN_FILES,
            name="REMOVE_ORPHAN_FILES",
            work_counters=(),
            no_work_reason="No orphan files were found.",
            no_work_reason_code=ReasonCode.NO_ORPHAN_FILES,
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


def disabled_operation(operation: CompactionOperation) -> OperationResult:
    return OperationResult(operation=operation, status=OperationStatus.DISABLED, reason_code=ReasonCode.DISABLED,
                           reason="Disabled by configuration.")


def skipped_operation(operation: CompactionOperation, reason_code: ReasonCode, reason: str) -> OperationResult:
    return OperationResult(operation=operation, status=OperationStatus.SKIPPED, reason_code=reason_code,
                           reason=reason)


def raised_operation(operation: CompactionOperation, error: BaseException) -> OperationResult:
    query = getattr(error, PROCEDURE_QUERY_ATTRIBUTE, None)
    if query is None:
        reason_code, reason = (ReasonCode.FAILED_OUTSIDE_PROCEDURE_CALL,
                               "The operation raised an error outside the Iceberg procedure call.")
    else:
        reason_code, reason = (ReasonCode.RESULT_UNREADABLE,
                               "The Iceberg procedure returned a result that could not be read.")
    return OperationResult(operation=operation, status=OperationStatus.FAILED, reason_code=reason_code, reason=reason,
                           query=query, error_type=type(error).__name__, error_message=str(error))


def classify_operation(operation: CompactionOperation, outcome: ProcedureOutcome, options: Optional[dict] = None,
                       before: Optional[TableState] = None, after: Optional[TableState] = None) -> OperationResult:
    spec = OPERATION_SPECS[operation]

    def result(status: OperationStatus, reason_code: ReasonCode, reason: str, **fields: Any) -> OperationResult:
        return OperationResult(operation=operation, status=status, reason_code=reason_code, reason=reason,
                               query=outcome.query, options=options, started_at=outcome.started_at,
                               ended_at=outcome.ended_at, **fields)

    if outcome.error is not None:
        if outcome.query is None:
            reason_code, reason = (ReasonCode.FAILED_BEFORE_PROCEDURE_CALL,
                                   "The operation failed before the Iceberg procedure was called.")
        else:
            reason_code, reason = ReasonCode.PROCEDURE_ERROR, "The Iceberg procedure call failed."
        return result(OperationStatus.FAILED, reason_code, reason,
                      error_type=type(outcome.error).__name__, error_message=str(outcome.error))

    rows = outcome.rows or []
    if spec.returns_file_rows:
        if rows:
            return result(OperationStatus.SUCCESS, ReasonCode.FILES_RETURNED,
                          f"Iceberg returned {len(rows)} orphan file location(s).", row_count=len(rows))
        return result(OperationStatus.NO_WORK, spec.no_work_reason_code, spec.no_work_reason, row_count=0)

    if not rows:
        return result(OperationStatus.FAILED, ReasonCode.RESULT_UNREADABLE, "Iceberg returned no result row.")
    metrics = rows[0].asDict()

    failed = _count(metrics, spec.failure_counter) if spec.failure_counter else 0
    committed = _count(metrics, spec.committed_counter) if spec.committed_counter else 0
    has_work = any(_count(metrics, key) > 0 for key in spec.work_counters)
    hidden = spec.commit_failures_hidden(options)

    if failed and committed:
        return result(OperationStatus.PARTIAL, ReasonCode.SOME_GROUPS_FAILED,
                      f"Iceberg committed {committed} rewritten data file(s) and reported {failed} data file(s) "
                      f"in failed rewrite groups.", metrics=metrics)
    if failed:
        return result(OperationStatus.FAILED, ReasonCode.REWRITE_GROUPS_FAILED,
                      f"Iceberg reported {failed} data file(s) in failed rewrite groups and committed none.",
                      metrics=metrics)
    if has_work:
        describe = spec.describe_work or _describe_counters
        notes = [PARTIAL_PROGRESS_NOTE] if hidden else []
        return result(OperationStatus.SUCCESS, ReasonCode.COMMITTED, describe(metrics), metrics=metrics, notes=notes)
    if spec.zero_requires_snapshot_evidence:
        status, reason_code, reason = _classify_zero_expiration(spec, before, after)
        return result(status, reason_code, reason, metrics=metrics)
    if hidden:
        return result(OperationStatus.UNVERIFIED, ReasonCode.ZERO_COMMITTED_UNDER_PARTIAL_PROGRESS,
                      ZERO_UNDER_PARTIAL_PROGRESS, metrics=metrics, notes=_window_notes(outcome, after))
    return result(OperationStatus.NO_WORK, spec.no_work_reason_code, spec.no_work_reason, metrics=metrics)


def _classify_zero_expiration(spec, before, after) -> tuple[OperationStatus, ReasonCode, str]:
    if before is None or after is None or before.snapshots is None or after.snapshots is None:
        return (OperationStatus.UNVERIFIED, ReasonCode.TABLE_STATE_UNAVAILABLE,
                "Iceberg reported no deleted files, and table state was not available to confirm that no "
                "snapshots expired.")
    remaining = {snapshot.snapshot_id for snapshot in after.snapshots}
    removed = [snapshot for snapshot in before.snapshots if snapshot.snapshot_id not in remaining]
    if removed:
        return (OperationStatus.UNVERIFIED, ReasonCode.SNAPSHOTS_REMOVED_WITHOUT_DELETED_FILES,
                f"Iceberg reported no deleted files, but {len(removed)} snapshot(s) present before maintenance "
                f"were absent afterwards; attribution unavailable.")
    return OperationStatus.NO_WORK, spec.no_work_reason_code, spec.no_work_reason


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
