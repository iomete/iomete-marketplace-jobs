import logging
from typing import Optional

from data_compaction_job.compaction_results import SnapshotInfo, TableState

logger = logging.getLogger(__name__)

SUMMARY_FIELDS = {
    "data_files": "total-data-files",
    "delete_files": "total-delete-files",
    "total_files_size": "total-files-size",
    "records": "total-records",
}


def read_table_state(spark, catalog: str, database: str, table: str) -> Optional[TableState]:
    identifier = f"`{catalog}`.`{database}`.`{table}`"
    try:
        properties = spark.sql(f"SHOW TBLPROPERTIES {identifier} ('current-snapshot-id')").collect()
        current_snapshot_id = _to_int(properties[0].value) if properties else None
        snapshot_rows = spark.sql(
            "SELECT snapshot_id, operation, unix_millis(committed_at) AS committed_at_ms "
            f"FROM {identifier}.snapshots").collect()
        summary_rows = [] if current_snapshot_id is None else spark.sql(
            f"SELECT summary FROM {identifier}.snapshots WHERE snapshot_id = {current_snapshot_id}").collect()
    except Exception as e:
        logger.warning(f"[{database}.{table}] Could not read table state for the summary: {e}")
        return None

    summary = (summary_rows[0].summary if summary_rows else None) or {}
    return TableState(
        snapshot_id=current_snapshot_id,
        snapshots=[SnapshotInfo(row.snapshot_id, row.operation, row.committed_at_ms) for row in snapshot_rows],
        **{attribute: _to_int(summary.get(key)) for attribute, key in SUMMARY_FIELDS.items()},
    )


def _to_int(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
