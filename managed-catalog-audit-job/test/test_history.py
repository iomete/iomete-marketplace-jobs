import unittest

from history import (
    FindingStatus,
    compare_findings,
    count_by_status,
    finding_fingerprint,
)
from models import Finding, Severity


def finding(
    *,
    rule_id="MC001",
    catalog="finance",
    storage=None,
    managers=(),
    consumers=(),
    details="details",
    evidence=(),
):
    return Finding(
        rule_id=rule_id,
        severity=Severity.HIGH,
        title="Test finding",
        catalog=catalog,
        storage=storage,
        managers=managers,
        consumers=consumers,
        evidence=evidence,
        details=details,
        impact="impact",
        recommendation="recommendation",
    )


class TestFindingFingerprint(unittest.TestCase):
    def test_same_finding_has_same_fingerprint(self):
        first = finding(
            rule_id="MC001",
            catalog="finance",
        )

        second = finding(
            rule_id="MC001",
            catalog="finance",
        )

        self.assertEqual(
            finding_fingerprint(first),
            finding_fingerprint(second),
        )

    def test_reporting_text_does_not_change_fingerprint(self):
        first = finding(
            details="old wording",
            evidence=("old evidence",),
        )

        second = finding(
            details="new improved wording",
            evidence=("different evidence",),
        )

        self.assertEqual(
            finding_fingerprint(first),
            finding_fingerprint(second),
        )

    def test_different_rule_has_different_fingerprint(self):
        first = finding(
            rule_id="MC001",
        )

        second = finding(
            rule_id="MC002",
            managers=("a2",),
        )

        self.assertNotEqual(
            finding_fingerprint(first),
            finding_fingerprint(second),
        )

    def test_mc001_manager_set_change_remains_same_issue(self):
        first = finding(
            rule_id="MC001",
            catalog="svc_sdp",
            managers=("a2_np", "s6"),
        )

        second = finding(
            rule_id="MC001",
            catalog="svc_sdp",
            managers=("a2_np", "a3", "s6"),
        )

        self.assertEqual(
            finding_fingerprint(first),
            finding_fingerprint(second),
        )

    def test_mc002_environment_is_part_of_identity(self):
        first = finding(
            rule_id="MC002",
            catalog="finance",
            managers=("a2",),
        )

        second = finding(
            rule_id="MC002",
            catalog="finance",
            managers=("a3",),
        )

        self.assertNotEqual(
            finding_fingerprint(first),
            finding_fingerprint(second),
        )

    def test_mc003_trailing_storage_slash_is_normalized(self):
        first = finding(
            rule_id="MC003",
            catalog="finance",
            storage="s3://finance-bucket",
        )

        second = finding(
            rule_id="MC003",
            catalog="finance",
            storage="s3://finance-bucket/",
        )

        self.assertEqual(
            finding_fingerprint(first),
            finding_fingerprint(second),
        )

    def test_mc003_consumer_changes_do_not_create_new_issue(self):
        first = finding(
            rule_id="MC003",
            catalog="finance",
            storage="s3://finance",
            consumers=("a2", "a3"),
        )

        second = finding(
            rule_id="MC003",
            catalog="finance",
            storage="s3://finance",
            consumers=("a2", "a3", "s6"),
        )

        self.assertEqual(
            finding_fingerprint(first),
            finding_fingerprint(second),
        )

    def test_mc004_consumer_is_part_of_identity(self):
        first = finding(
            rule_id="MC004",
            catalog="finance_external",
            storage="s3://finance",
            consumers=("a2",),
        )

        second = finding(
            rule_id="MC004",
            catalog="finance_external",
            storage="s3://finance",
            consumers=("a3",),
        )

        self.assertNotEqual(
            finding_fingerprint(first),
            finding_fingerprint(second),
        )

    def test_mc004_storage_trailing_slash_is_normalized(self):
        first = finding(
            rule_id="MC004",
            catalog="finance_external",
            storage="s3://finance",
            consumers=("a2",),
        )

        second = finding(
            rule_id="MC004",
            catalog="finance_external",
            storage="s3://finance/",
            consumers=("a2",),
        )

        self.assertEqual(
            finding_fingerprint(first),
            finding_fingerprint(second),
        )


class TestFindingComparison(unittest.TestCase):
    def test_first_run_marks_everything_new(self):
        current = [
            finding(
                catalog="finance",
            ),
            finding(
                catalog="sales",
            ),
        ]

        deltas = compare_findings(
            current_findings=current,
            previous_findings=[],
        )

        self.assertEqual(
            len(deltas),
            2,
        )

        self.assertTrue(all(delta.status == FindingStatus.NEW for delta in deltas))

    def test_existing_finding_is_ongoing(self):
        previous = [
            finding(
                catalog="finance",
                details="old details",
            )
        ]

        current = [
            finding(
                catalog="finance",
                details="new details",
            )
        ]

        deltas = compare_findings(
            current_findings=current,
            previous_findings=previous,
        )

        self.assertEqual(
            len(deltas),
            1,
        )

        self.assertEqual(
            deltas[0].status,
            FindingStatus.ONGOING,
        )

    def test_missing_current_finding_is_resolved(self):
        previous = [
            finding(
                catalog="finance",
            )
        ]

        deltas = compare_findings(
            current_findings=[],
            previous_findings=previous,
        )

        self.assertEqual(
            len(deltas),
            1,
        )

        self.assertEqual(
            deltas[0].status,
            FindingStatus.RESOLVED,
        )

    def test_mixed_weekly_changes(self):
        previous = [
            finding(
                catalog="finance",
            ),
            finding(
                catalog="sales",
            ),
        ]

        current = [
            finding(
                catalog="finance",
            ),
            finding(
                catalog="supply_chain",
            ),
        ]

        deltas = compare_findings(
            current_findings=current,
            previous_findings=previous,
        )

        statuses = {delta.finding.catalog: delta.status for delta in deltas}

        self.assertEqual(
            statuses["finance"],
            FindingStatus.ONGOING,
        )

        self.assertEqual(
            statuses["supply_chain"],
            FindingStatus.NEW,
        )

        self.assertEqual(
            statuses["sales"],
            FindingStatus.RESOLVED,
        )

    def test_duplicate_current_identity_fails_loudly(self):
        current = [
            finding(
                catalog="finance",
            ),
            finding(
                catalog="finance",
            ),
        ]

        with self.assertRaises(ValueError):
            compare_findings(
                current_findings=current,
                previous_findings=[],
            )

    def test_status_counts(self):
        previous = [
            finding(
                catalog="finance",
            ),
            finding(
                catalog="sales",
            ),
        ]

        current = [
            finding(
                catalog="finance",
            ),
            finding(
                catalog="supply_chain",
            ),
        ]

        deltas = compare_findings(
            current_findings=current,
            previous_findings=previous,
        )

        counts = count_by_status(deltas)

        self.assertEqual(
            counts[FindingStatus.NEW],
            1,
        )

        self.assertEqual(
            counts[FindingStatus.ONGOING],
            1,
        )

        self.assertEqual(
            counts[FindingStatus.RESOLVED],
            1,
        )

    def test_partial_scan_does_not_mark_missing_finding_resolved(self):
        previous = [
            finding(
                catalog="finance",
            )
        ]

        deltas = compare_findings(
            current_findings=[],
            previous_findings=previous,
            current_scan_complete=False,
        )

        self.assertEqual(
            deltas,
            [],
        )


    def test_partial_scan_still_marks_observed_findings_new_or_ongoing(self):
        previous = [
            finding(
                catalog="finance",
            ),
            finding(
                catalog="sales",
            ),
        ]

        current = [
            finding(
                catalog="finance",
            ),
            finding(
                catalog="supply_chain",
            ),
        ]

        deltas = compare_findings(
            current_findings=current,
            previous_findings=previous,
            current_scan_complete=False,
        )

        statuses = {
            delta.finding.catalog: delta.status
            for delta in deltas
        }

        self.assertEqual(
            statuses,
            {
                "finance": FindingStatus.ONGOING,
                "supply_chain": FindingStatus.NEW,
            },
        )

if __name__ == "__main__":
    unittest.main()
