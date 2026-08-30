import unittest
from datetime import datetime, timezone

from history import FindingStatus
from persistence import (
    FINDINGS_TABLE,
    RUNS_TABLE,
    AuditFindingRecord,
    AuditRunRecord,
)
from spark_persistence import SparkAuditPersistence


class FakeQueryResult:
    def __init__(
        self,
        rows=None,
    ):
        self.rows = rows or []

    def collect(self):
        return self.rows


class FakeSpark:
    def __init__(
        self,
        results=None,
    ):
        self.results = list(results or [])

        self.queries = []

    def sql(
        self,
        query,
    ):
        self.queries.append(query)

        if self.results:
            return FakeQueryResult(self.results.pop(0))

        return FakeQueryResult()


def run_record():
    return AuditRunRecord(
        run_id="run-123",
        started_at=datetime(
            2026,
            8,
            29,
            19,
            55,
            tzinfo=timezone.utc,
        ),
        completed_at=datetime(
            2026,
            8,
            29,
            20,
            0,
            tzinfo=timezone.utc,
        ),
        scan_status="COMPLETE",
        configured_environment_count=20,
        successful_environment_count=20,
        failed_environment_count=0,
        catalog_count=675,
        high_count=60,
        warning_count=0,
        recommendation_count=11,
        total_finding_count=71,
        failed_environments=(),
    )


def finding_record(
    *,
    status=FindingStatus.NEW,
    catalog="finance",
    title="Test finding",
):
    return AuditFindingRecord(
        run_id="run-123",
        finding_fingerprint="abc123",
        finding_status=status,
        rule_id="MC001",
        severity="HIGH",
        title=title,
        catalog=catalog,
        storage="s3://finance",
        managers=("a2", "a3"),
        consumers=("s6",),
        evidence=("evidence",),
        details="details",
        impact="impact",
        recommendation="recommendation",
        observed_at=datetime(
            2026,
            8,
            29,
            20,
            0,
            tzinfo=timezone.utc,
        ),
    )


class TestEnsureTables(unittest.TestCase):
    def test_creates_both_audit_tables(self):
        spark = FakeSpark()

        persistence = SparkAuditPersistence(spark)

        persistence.ensure_tables()

        self.assertEqual(
            len(spark.queries),
            2,
        )

        self.assertIn(
            f"CREATE TABLE IF NOT EXISTS {RUNS_TABLE}",
            spark.queries[0],
        )

        self.assertIn(
            "USING iceberg",
            spark.queries[0],
        )

        self.assertIn(
            f"CREATE TABLE IF NOT EXISTS {FINDINGS_TABLE}",
            spark.queries[1],
        )

        self.assertIn(
            "USING iceberg",
            spark.queries[1],
        )


class TestPreviousRunLoading(unittest.TestCase):
    def test_returns_none_when_no_complete_run_exists(self):
        spark = FakeSpark(
            results=[
                [],
            ]
        )

        persistence = SparkAuditPersistence(spark)

        result = persistence.load_previous_complete_run()

        self.assertIsNone(result)

    def test_queries_only_complete_runs(self):
        spark = FakeSpark(
            results=[
                [],
            ]
        )

        persistence = SparkAuditPersistence(spark)

        persistence.load_previous_complete_run()

        query = spark.queries[0]

        self.assertIn(
            "WHERE scan_status = 'COMPLETE'",
            query,
        )

        self.assertIn(
            "ORDER BY completed_at DESC",
            query,
        )

        self.assertIn(
            "LIMIT 1",
            query,
        )

    def test_reconstructs_previous_run_record(self):
        row = {
            "run_id": "previous-run",
            "started_at": datetime(
                2026,
                8,
                22,
                19,
                55,
            ),
            "completed_at": datetime(
                2026,
                8,
                22,
                20,
                0,
            ),
            "scan_status": "COMPLETE",
            "configured_environment_count": 20,
            "successful_environment_count": 20,
            "failed_environment_count": 0,
            "catalog_count": 670,
            "high_count": 59,
            "warning_count": 0,
            "recommendation_count": 11,
            "total_finding_count": 70,
            "failed_environments": [],
        }

        spark = FakeSpark(
            results=[
                [row],
            ]
        )

        persistence = SparkAuditPersistence(spark)

        result = persistence.load_previous_complete_run()

        self.assertIsNotNone(result)
        assert result is not None

        self.assertEqual(
            result.run_id,
            "previous-run",
        )

        self.assertEqual(
            result.scan_status,
            "COMPLETE",
        )

        self.assertEqual(
            result.catalog_count,
            670,
        )

        self.assertEqual(
            result.failed_environments,
            (),
        )


class TestFindingLoading(unittest.TestCase):
    def test_loads_finding_records_for_run(self):
        observed_at = datetime(
            2026,
            8,
            22,
            20,
            0,
        )

        row = {
            "run_id": "previous-run",
            "finding_fingerprint": "fingerprint-1",
            "finding_status": "ONGOING",
            "rule_id": "MC001",
            "severity": "HIGH",
            "title": "Ownership conflict",
            "catalog": "finance",
            "storage": "s3://finance",
            "managers": ["a2", "a3"],
            "consumers": ["s6"],
            "evidence": ["evidence"],
            "details": "details",
            "impact": "impact",
            "recommendation": "recommendation",
            "observed_at": observed_at,
        }

        spark = FakeSpark(
            results=[
                [row],
            ]
        )

        persistence = SparkAuditPersistence(spark)

        records = persistence.load_findings_for_run("previous-run")

        self.assertEqual(
            len(records),
            1,
        )

        self.assertEqual(
            records[0].finding_status,
            FindingStatus.ONGOING,
        )

        self.assertEqual(
            records[0].catalog,
            "finance",
        )

        self.assertEqual(
            records[0].managers,
            (
                "a2",
                "a3",
            ),
        )

        self.assertIn(
            "WHERE run_id = 'previous-run'",
            spark.queries[0],
        )


class TestRunSaving(unittest.TestCase):
    def test_saves_run_record(self):
        spark = FakeSpark()

        persistence = SparkAuditPersistence(spark)

        persistence.save_run(run_record())

        self.assertEqual(
            len(spark.queries),
            1,
        )

        query = spark.queries[0]

        self.assertIn(
            f"INSERT INTO {RUNS_TABLE}",
            query,
        )

        self.assertIn(
            "'run-123'",
            query,
        )

        self.assertIn(
            "'COMPLETE'",
            query,
        )

        self.assertIn(
            "675",
            query,
        )

    def test_failed_environment_names_are_written(self):
        spark = FakeSpark()

        record = run_record()

        record = AuditRunRecord(
            run_id=record.run_id,
            started_at=record.started_at,
            completed_at=record.completed_at,
            scan_status="PARTIAL",
            configured_environment_count=20,
            successful_environment_count=19,
            failed_environment_count=1,
            catalog_count=650,
            high_count=55,
            warning_count=0,
            recommendation_count=10,
            total_finding_count=65,
            failed_environments=("r4_ge4_np",),
        )

        persistence = SparkAuditPersistence(spark)

        persistence.save_run(record)

        query = spark.queries[0]

        self.assertIn(
            "array('r4_ge4_np')",
            query,
        )


class TestFindingSaving(unittest.TestCase):
    def test_empty_findings_do_not_issue_insert(self):
        spark = FakeSpark()

        persistence = SparkAuditPersistence(spark)

        persistence.save_findings([])

        self.assertEqual(
            spark.queries,
            [],
        )

    def test_saves_findings_in_single_insert(self):
        spark = FakeSpark()

        persistence = SparkAuditPersistence(spark)

        records = [
            finding_record(
                catalog="finance",
            ),
            finding_record(
                status=FindingStatus.ONGOING,
                catalog="sales",
            ),
        ]

        persistence.save_findings(records)

        self.assertEqual(
            len(spark.queries),
            1,
        )

        query = spark.queries[0]

        self.assertIn(
            f"INSERT INTO {FINDINGS_TABLE}",
            query,
        )

        self.assertIn(
            "'finance'",
            query,
        )

        self.assertIn(
            "'sales'",
            query,
        )

        self.assertIn(
            "'NEW'",
            query,
        )

        self.assertIn(
            "'ONGOING'",
            query,
        )

    def test_strings_are_sql_escaped(self):
        spark = FakeSpark()

        persistence = SparkAuditPersistence(spark)

        record = finding_record(
            title="Catalog owner's conflict",
        )

        persistence.save_findings([record])

        self.assertIn(
            "Catalog owner''s conflict",
            spark.queries[0],
        )


if __name__ == "__main__":
    unittest.main()
