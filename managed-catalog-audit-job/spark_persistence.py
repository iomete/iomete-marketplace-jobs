from datetime import datetime, timezone
from typing import Any, Iterable

from history import FindingStatus
from persistence import (
    FINDINGS_TABLE,
    RUNS_TABLE,
    AuditFindingRecord,
    AuditRunRecord,
)


class SparkAuditPersistence:
    """
    Persist managed catalog audit history in Iceberg tables through Spark SQL.

    This class intentionally contains only Spark/Iceberg concerns.
    Finding comparison and status calculation remain in history.py.
    """

    def __init__(self, spark: Any):
        self.spark = spark

    def ensure_tables(self) -> None:
        """
        Create the audit tables when they do not already exist.
        """
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
                run_id STRING,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                scan_status STRING,
                configured_environment_count BIGINT,
                successful_environment_count BIGINT,
                failed_environment_count BIGINT,
                catalog_count BIGINT,
                high_count BIGINT,
                warning_count BIGINT,
                recommendation_count BIGINT,
                total_finding_count BIGINT,
                failed_environments ARRAY<STRING>
            )
            USING iceberg
            PARTITIONED BY (days(completed_at))
            """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {FINDINGS_TABLE} (
                run_id STRING,
                finding_fingerprint STRING,
                finding_status STRING,
                rule_id STRING,
                severity STRING,
                title STRING,
                catalog STRING,
                storage STRING,
                managers ARRAY<STRING>,
                consumers ARRAY<STRING>,
                evidence ARRAY<STRING>,
                details STRING,
                impact STRING,
                recommendation STRING,
                observed_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(observed_at))
            """)

    def load_previous_complete_run(
        self,
    ) -> AuditRunRecord | None:
        """
        Return the newest complete audit run.

        Partial runs are deliberately excluded because they must not become
        the baseline used to decide that a finding was resolved.
        """
        rows = self.spark.sql(f"""
            SELECT
                run_id,
                started_at,
                completed_at,
                scan_status,
                configured_environment_count,
                successful_environment_count,
                failed_environment_count,
                catalog_count,
                high_count,
                warning_count,
                recommendation_count,
                total_finding_count,
                failed_environments
            FROM {RUNS_TABLE}
            WHERE scan_status = 'COMPLETE'
            ORDER BY completed_at DESC
            LIMIT 1
            """).collect()

        if not rows:
            return None

        row = rows[0]

        return AuditRunRecord(
            run_id=_row_value(row, "run_id"),
            started_at=_row_value(row, "started_at"),
            completed_at=_row_value(row, "completed_at"),
            scan_status=_row_value(row, "scan_status"),
            configured_environment_count=int(
                _row_value(
                    row,
                    "configured_environment_count",
                )
            ),
            successful_environment_count=int(
                _row_value(
                    row,
                    "successful_environment_count",
                )
            ),
            failed_environment_count=int(
                _row_value(
                    row,
                    "failed_environment_count",
                )
            ),
            catalog_count=int(
                _row_value(
                    row,
                    "catalog_count",
                )
            ),
            high_count=int(
                _row_value(
                    row,
                    "high_count",
                )
            ),
            warning_count=int(
                _row_value(
                    row,
                    "warning_count",
                )
            ),
            recommendation_count=int(
                _row_value(
                    row,
                    "recommendation_count",
                )
            ),
            total_finding_count=int(
                _row_value(
                    row,
                    "total_finding_count",
                )
            ),
            failed_environments=_as_tuple(
                _row_value(
                    row,
                    "failed_environments",
                )
            ),
        )

    def load_findings_for_run(
        self,
        run_id: str,
    ) -> list[AuditFindingRecord]:
        """
        Load all finding records associated with one audit run.
        """
        rows = self.spark.sql(f"""
            SELECT
                run_id,
                finding_fingerprint,
                finding_status,
                rule_id,
                severity,
                title,
                catalog,
                storage,
                managers,
                consumers,
                evidence,
                details,
                impact,
                recommendation,
                observed_at
            FROM {FINDINGS_TABLE}
            WHERE run_id = {_sql_string(run_id)}
            ORDER BY
                rule_id,
                catalog,
                finding_fingerprint
            """).collect()

        return [
            AuditFindingRecord(
                run_id=_row_value(row, "run_id"),
                finding_fingerprint=_row_value(
                    row,
                    "finding_fingerprint",
                ),
                finding_status=FindingStatus(
                    _row_value(
                        row,
                        "finding_status",
                    )
                ),
                rule_id=_row_value(
                    row,
                    "rule_id",
                ),
                severity=_row_value(
                    row,
                    "severity",
                ),
                title=_row_value(
                    row,
                    "title",
                ),
                catalog=_row_value(
                    row,
                    "catalog",
                ),
                storage=_row_value(
                    row,
                    "storage",
                ),
                managers=_as_tuple(
                    _row_value(
                        row,
                        "managers",
                    )
                ),
                consumers=_as_tuple(
                    _row_value(
                        row,
                        "consumers",
                    )
                ),
                evidence=_as_tuple(
                    _row_value(
                        row,
                        "evidence",
                    )
                ),
                details=_row_value(
                    row,
                    "details",
                ),
                impact=_row_value(
                    row,
                    "impact",
                ),
                recommendation=_row_value(
                    row,
                    "recommendation",
                ),
                observed_at=_row_value(
                    row,
                    "observed_at",
                ),
            )
            for row in rows
        ]

    def save_run(
        self,
        record: AuditRunRecord,
    ) -> None:
        """
        Append one audit-run summary row.
        """
        self.spark.sql(f"""
            INSERT INTO {RUNS_TABLE} (
                run_id,
                started_at,
                completed_at,
                scan_status,
                configured_environment_count,
                successful_environment_count,
                failed_environment_count,
                catalog_count,
                high_count,
                warning_count,
                recommendation_count,
                total_finding_count,
                failed_environments
            )
            VALUES (
                {_sql_string(record.run_id)},
                {_sql_timestamp(record.started_at)},
                {_sql_timestamp(record.completed_at)},
                {_sql_string(record.scan_status)},
                {record.configured_environment_count},
                {record.successful_environment_count},
                {record.failed_environment_count},
                {record.catalog_count},
                {record.high_count},
                {record.warning_count},
                {record.recommendation_count},
                {record.total_finding_count},
                {_sql_array(record.failed_environments)}
            )
            """)

    def save_findings(
        self,
        records: Iterable[AuditFindingRecord],
    ) -> None:
        """
        Append finding rows for one audit execution.

        All records are inserted in one Spark SQL statement rather than
        issuing one query for every finding.
        """
        records = list(records)

        if not records:
            return

        values = ",\n".join(_finding_values(record) for record in records)

        self.spark.sql(f"""
            INSERT INTO {FINDINGS_TABLE} (
                run_id,
                finding_fingerprint,
                finding_status,
                rule_id,
                severity,
                title,
                catalog,
                storage,
                managers,
                consumers,
                evidence,
                details,
                impact,
                recommendation,
                observed_at
            )
            VALUES
            {values}
            """)


def _finding_values(
    record: AuditFindingRecord,
) -> str:
    return f"""(
        {_sql_string(record.run_id)},
        {_sql_string(record.finding_fingerprint)},
        {_sql_string(record.finding_status.value)},
        {_sql_string(record.rule_id)},
        {_sql_string(record.severity)},
        {_sql_string(record.title)},
        {_sql_optional_string(record.catalog)},
        {_sql_optional_string(record.storage)},
        {_sql_array(record.managers)},
        {_sql_array(record.consumers)},
        {_sql_array(record.evidence)},
        {_sql_string(record.details)},
        {_sql_string(record.impact)},
        {_sql_string(record.recommendation)},
        {_sql_timestamp(record.observed_at)}
    )"""


def _sql_string(
    value: str,
) -> str:
    escaped = value.replace(
        "'",
        "''",
    )

    return f"'{escaped}'"


def _sql_optional_string(
    value: str | None,
) -> str:
    if value is None:
        return "NULL"

    return _sql_string(value)


def _sql_array(
    values: Iterable[str],
) -> str:
    values = list(values)

    if not values:
        return "CAST(array() AS ARRAY<STRING>)"

    items = ", ".join(_sql_string(value) for value in values)

    return f"array({items})"


def _sql_timestamp(
    value: datetime,
) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)

    formatted = value.strftime("%Y-%m-%d %H:%M:%S.%f")

    return f"TIMESTAMP '{formatted}'"


def _row_value(
    row: Any,
    field: str,
) -> Any:
    if isinstance(row, dict):
        return row[field]

    try:
        return row[field]
    except (KeyError, TypeError):
        return getattr(
            row,
            field,
        )


def _as_tuple(
    value: Any,
) -> tuple[str, ...]:
    if value is None:
        return ()

    return tuple(value)
