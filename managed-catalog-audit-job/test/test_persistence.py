import unittest
from datetime import datetime, timezone

from history import FindingDelta, FindingStatus, finding_fingerprint
from models import Finding, Severity
from persistence import (
    FINDINGS_TABLE,
    RUNS_TABLE,
    AuditFindingRecord,
    build_finding_record,
    build_finding_records,
    current_findings_from_records,
)


def finding(
    *,
    rule_id="MC001",
    severity=Severity.HIGH,
    title="Test finding",
    catalog="finance",
    storage=None,
    managers=(),
    consumers=(),
    evidence=(),
    details="details",
    impact="impact",
    recommendation="recommendation",
):
    return Finding(
        rule_id=rule_id,
        severity=severity,
        title=title,
        catalog=catalog,
        storage=storage,
        managers=managers,
        consumers=consumers,
        evidence=evidence,
        details=details,
        impact=impact,
        recommendation=recommendation,
    )


def delta(
    *,
    status=FindingStatus.NEW,
    value=None,
):
    value = value or finding()

    return FindingDelta(
        fingerprint=finding_fingerprint(value),
        status=status,
        finding=value,
    )


class TestPersistenceConstants(unittest.TestCase):
    def test_runs_table_uses_system_database(self):
        self.assertEqual(
            RUNS_TABLE,
            ("spark_catalog.iomete_system_db." "managed_catalog_audit_runs"),
        )

    def test_findings_table_uses_system_database(self):
        self.assertEqual(
            FINDINGS_TABLE,
            ("spark_catalog.iomete_system_db." "managed_catalog_audit_findings"),
        )


class TestFindingRecordConversion(unittest.TestCase):
    def test_build_finding_record_preserves_fields(self):
        value = finding(
            rule_id="MC004",
            severity=Severity.HIGH,
            title="External target cannot be resolved",
            catalog="finance_external",
            storage="s3://finance",
            managers=("a2",),
            consumers=("a3",),
            evidence=("target=finance",),
            details="Target could not be resolved.",
            impact="Topology cannot be verified.",
            recommendation="Review the external target.",
        )

        finding_delta = delta(
            status=FindingStatus.NEW,
            value=value,
        )

        observed_at = datetime(
            2026,
            8,
            29,
            20,
            0,
            tzinfo=timezone.utc,
        )

        record = build_finding_record(
            run_id="run-123",
            delta=finding_delta,
            observed_at=observed_at,
        )

        self.assertEqual(
            record.run_id,
            "run-123",
        )

        self.assertEqual(
            record.finding_fingerprint,
            finding_delta.fingerprint,
        )

        self.assertEqual(
            record.finding_status,
            FindingStatus.NEW,
        )

        self.assertEqual(
            record.rule_id,
            "MC004",
        )

        self.assertEqual(
            record.severity,
            "HIGH",
        )

        self.assertEqual(
            record.title,
            "External target cannot be resolved",
        )

        self.assertEqual(
            record.catalog,
            "finance_external",
        )

        self.assertEqual(
            record.storage,
            "s3://finance",
        )

        self.assertEqual(
            record.managers,
            ("a2",),
        )

        self.assertEqual(
            record.consumers,
            ("a3",),
        )

        self.assertEqual(
            record.evidence,
            ("target=finance",),
        )

        self.assertEqual(
            record.details,
            "Target could not be resolved.",
        )

        self.assertEqual(
            record.impact,
            "Topology cannot be verified.",
        )

        self.assertEqual(
            record.recommendation,
            "Review the external target.",
        )

        self.assertEqual(
            record.observed_at,
            observed_at,
        )

    def test_build_multiple_finding_records(self):
        first = delta(
            value=finding(
                catalog="finance",
            )
        )

        second = delta(
            status=FindingStatus.ONGOING,
            value=finding(
                catalog="sales",
            ),
        )

        observed_at = datetime(
            2026,
            8,
            29,
            20,
            0,
            tzinfo=timezone.utc,
        )

        records = build_finding_records(
            run_id="run-123",
            deltas=[
                first,
                second,
            ],
            observed_at=observed_at,
        )

        self.assertEqual(
            len(records),
            2,
        )

        self.assertEqual(
            records[0].finding_status,
            FindingStatus.NEW,
        )

        self.assertEqual(
            records[1].finding_status,
            FindingStatus.ONGOING,
        )

        self.assertTrue(all(record.run_id == "run-123" for record in records))


class TestPreviousFindingReconstruction(unittest.TestCase):
    def _record(
        self,
        *,
        status,
        catalog,
        rule_id="MC001",
    ):
        return AuditFindingRecord(
            run_id="run-previous",
            finding_fingerprint="fingerprint",
            finding_status=status,
            rule_id=rule_id,
            severity="HIGH",
            title="Test finding",
            catalog=catalog,
            storage="s3://bucket",
            managers=("a2",),
            consumers=("a3",),
            evidence=("evidence",),
            details="details",
            impact="impact",
            recommendation="recommendation",
            observed_at=datetime(
                2026,
                8,
                22,
                20,
                0,
                tzinfo=timezone.utc,
            ),
        )

    def test_new_record_is_active_in_next_baseline(self):
        records = [
            self._record(
                status=FindingStatus.NEW,
                catalog="finance",
            )
        ]

        findings = current_findings_from_records(records)

        self.assertEqual(
            len(findings),
            1,
        )

        self.assertEqual(
            findings[0].catalog,
            "finance",
        )

    def test_ongoing_record_is_active_in_next_baseline(self):
        records = [
            self._record(
                status=FindingStatus.ONGOING,
                catalog="finance",
            )
        ]

        findings = current_findings_from_records(records)

        self.assertEqual(
            len(findings),
            1,
        )

        self.assertEqual(
            findings[0].catalog,
            "finance",
        )

    def test_resolved_record_is_excluded_from_next_baseline(self):
        records = [
            self._record(
                status=FindingStatus.RESOLVED,
                catalog="finance",
            )
        ]

        findings = current_findings_from_records(records)

        self.assertEqual(
            findings,
            [],
        )

    def test_mixed_records_only_return_active_findings(self):
        records = [
            self._record(
                status=FindingStatus.NEW,
                catalog="finance",
            ),
            self._record(
                status=FindingStatus.ONGOING,
                catalog="sales",
            ),
            self._record(
                status=FindingStatus.RESOLVED,
                catalog="hr",
            ),
        ]

        findings = current_findings_from_records(records)

        catalogs = {value.catalog for value in findings}

        self.assertEqual(
            catalogs,
            {
                "finance",
                "sales",
            },
        )

    def test_reconstructed_finding_preserves_severity(self):
        records = [
            self._record(
                status=FindingStatus.NEW,
                catalog="finance",
            )
        ]

        findings = current_findings_from_records(records)

        self.assertEqual(
            findings[0].severity,
            Severity.HIGH,
        )


if __name__ == "__main__":
    unittest.main()
