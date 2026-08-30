from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from history import FindingDelta, FindingStatus
from models import Finding, Severity

SYSTEM_DATABASE = "spark_catalog.iomete_system_db"

RUNS_TABLE = f"{SYSTEM_DATABASE}.managed_catalog_audit_runs"

FINDINGS_TABLE = f"{SYSTEM_DATABASE}.managed_catalog_audit_findings"


@dataclass(frozen=True)
class AuditRunRecord:
    run_id: str
    started_at: datetime
    completed_at: datetime

    scan_status: str

    configured_environment_count: int
    successful_environment_count: int
    failed_environment_count: int

    catalog_count: int

    high_count: int
    warning_count: int
    recommendation_count: int
    total_finding_count: int

    failed_environments: tuple[str, ...]


@dataclass(frozen=True)
class AuditFindingRecord:
    run_id: str
    finding_fingerprint: str
    finding_status: FindingStatus

    rule_id: str
    severity: str
    title: str

    catalog: str | None
    storage: str | None

    managers: tuple[str, ...]
    consumers: tuple[str, ...]

    evidence: tuple[str, ...]

    details: str
    impact: str
    recommendation: str

    observed_at: datetime


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_finding_record(
    run_id: str,
    delta: FindingDelta,
    observed_at: datetime,
) -> AuditFindingRecord:
    finding = delta.finding

    return AuditFindingRecord(
        run_id=run_id,
        finding_fingerprint=delta.fingerprint,
        finding_status=delta.status,
        rule_id=finding.rule_id,
        severity=finding.severity.value,
        title=finding.title,
        catalog=finding.catalog,
        storage=finding.storage,
        managers=tuple(finding.managers),
        consumers=tuple(finding.consumers),
        evidence=tuple(finding.evidence),
        details=finding.details,
        impact=finding.impact,
        recommendation=finding.recommendation,
        observed_at=observed_at,
    )


def build_finding_records(
    run_id: str,
    deltas: Iterable[FindingDelta],
    observed_at: datetime,
) -> list[AuditFindingRecord]:
    return [
        build_finding_record(
            run_id=run_id,
            delta=delta,
            observed_at=observed_at,
        )
        for delta in deltas
    ]


def current_findings_from_records(
    records: Iterable[AuditFindingRecord],
) -> list[Finding]:
    """
    Reconstruct findings that were active in a previous run.

    RESOLVED records are excluded because they represent findings that
    disappeared during that run and must not become part of the next
    comparison baseline.
    """
    findings = []

    for record in records:
        if record.finding_status == FindingStatus.RESOLVED:
            continue

        findings.append(
            Finding(
                rule_id=record.rule_id,
                severity=Severity(record.severity),
                title=record.title,
                catalog=record.catalog,
                storage=record.storage,
                managers=record.managers,
                consumers=record.consumers,
                evidence=record.evidence,
                details=record.details,
                impact=record.impact,
                recommendation=record.recommendation,
            )
        )

    return findings
