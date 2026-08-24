import contextlib
import io
import os
import tempfile
import unittest

import polars as pl

from models import (
    Finding,
    InventoryResult,
    ScanFailure,
    Severity,
)
from reporting import (
    export_csv_reports,
    export_markdown_report,
    print_terminal_report,
)


def sample_result():
    return InventoryResult(
        inventory=pl.DataFrame(
            [
                {
                    "name": "finance",
                    "env_name": "a2",
                }
            ]
        ),
        configured_count=2,
        successful_environments=(
            "a2",
            "a3",
        ),
        failures=(),
    )


def sample_finding():
    return Finding(
        rule_id="MC001",
        severity=Severity.HIGH,
        title="Catalog ownership conflict",
        catalog="finance",
        managers=("a2", "a3"),
        evidence=(
            "a2 / finance / internal",
            "a3 / finance / internal",
        ),
        details="Same catalog is internal twice.",
        impact="Ownership is ambiguous.",
        recommendation="Choose one manager.",
    )


class TestTerminalReporting(unittest.TestCase):
    def test_default_output_is_summary_only(self):
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            print_terminal_report(
                sample_result(),
                [sample_finding()],
                verbose=False,
            )

        text = output.getvalue()

        self.assertIn(
            "MANAGED CATALOG AUDIT",
            text,
        )

        self.assertIn(
            "MC001",
            text,
        )

        self.assertNotIn(
            "DETAILED FINDINGS",
            text,
        )

    def test_verbose_output_contains_details(self):
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            print_terminal_report(
                sample_result(),
                [sample_finding()],
                verbose=True,
            )

        text = output.getvalue()

        self.assertIn(
            "DETAILED FINDINGS",
            text,
        )

        self.assertIn(
            "Ownership is ambiguous.",
            text,
        )

        self.assertIn(
            "a2 / finance / internal",
            text,
        )

    def test_partial_scan_is_visible(self):
        result = InventoryResult(
            inventory=pl.DataFrame(),
            configured_count=2,
            successful_environments=("a2",),
            failures=(
                ScanFailure(
                    environment="a3",
                    error_type="TimeoutError",
                    message="timed out",
                ),
            ),
        )

        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            print_terminal_report(
                result,
                [],
            )

        text = output.getvalue()

        self.assertIn(
            "PARTIAL",
            text,
        )

        self.assertIn(
            "a3",
            text,
        )

        self.assertIn(
            "TimeoutError",
            text,
        )


class TestMarkdownReporting(unittest.TestCase):
    def test_markdown_contains_full_finding_context(self):
        with tempfile.TemporaryDirectory() as directory:
            output_file = os.path.join(
                directory,
                "audit.md",
            )

            export_markdown_report(
                sample_result(),
                [sample_finding()],
                output_file=output_file,
            )

            with open(
                output_file,
                encoding="utf-8",
            ) as file:
                content = file.read()

            self.assertIn(
                "# Managed Catalog Audit",
                content,
            )

            self.assertIn(
                "MC001",
                content,
            )

            self.assertIn(
                "What was detected",
                content,
            )

            self.assertIn(
                "Why it matters",
                content,
            )

            self.assertIn(
                "Recommended action",
                content,
            )

            self.assertIn(
                "a2 / finance / internal",
                content,
            )


class TestCsvReporting(unittest.TestCase):
    def test_findings_csv_contains_expected_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            findings_file = os.path.join(
                directory,
                "findings.csv",
            )

            inventory_file = os.path.join(
                directory,
                "inventory.csv",
            )

            inventory = sample_result().inventory

            export_csv_reports(
                inventory,
                [sample_finding()],
                findings_file=findings_file,
                inventory_file=inventory_file,
            )

            findings_df = pl.read_csv(findings_file)

            self.assertEqual(
                findings_df.columns,
                [
                    "rule_id",
                    "severity",
                    "title",
                    "catalog",
                    "storage",
                    "managers",
                    "consumers",
                    "evidence",
                    "details",
                    "impact",
                    "recommendation",
                ],
            )

    def test_empty_findings_still_produces_valid_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            findings_file = os.path.join(
                directory,
                "findings.csv",
            )

            inventory_file = os.path.join(
                directory,
                "inventory.csv",
            )

            export_csv_reports(
                sample_result().inventory,
                [],
                findings_file=findings_file,
                inventory_file=inventory_file,
            )

            df = pl.read_csv(findings_file)

            self.assertEqual(
                len(df),
                0,
            )

            self.assertIn(
                "rule_id",
                df.columns,
            )


if __name__ == "__main__":
    unittest.main()
