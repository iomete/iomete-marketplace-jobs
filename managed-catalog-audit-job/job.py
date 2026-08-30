import argparse
from datetime import datetime, timezone
from uuid import uuid4

from config import load_environments
from history import compare_findings
from inventory import fetch_catalog_inventory
from models import Severity
from persistence import (
    AuditRunRecord,
    build_finding_records,
    current_findings_from_records,
)
from reporting import (
    RULE_METADATA,
    export_csv_reports,
    export_markdown_report,
    get_rule_key,
    print_report_files,
    print_terminal_report,
)
from rules import run_rules
from spark_persistence import SparkAuditPersistence


def available_rule_keys() -> list[str]:
    return sorted(metadata["key"] for metadata in RULE_METADATA.values())


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Audit managed catalog topology and "
            "configuration across IOMETE environments."
        )
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help=(
            "Print all detailed findings to the terminal. "
            "Detailed findings are always written to the "
            "CSV and Markdown reports."
        ),
    )

    parser.add_argument(
        "--rule",
        action="append",
        choices=available_rule_keys(),
        help=(
            "Include findings from only the specified rule. "
            "Use the human-readable rule key rather than "
            "MC001/MC002. This option can be repeated to "
            "select multiple rules. If omitted, all rules "
            "are included."
        ),
    )

    return parser.parse_args()


def filter_findings_by_rule(
    findings,
    selected_rules,
):
    if not selected_rules:
        return findings

    selected_rules = set(selected_rules)

    return [
        finding
        for finding in findings
        if get_rule_key(finding.rule_id) in selected_rules
    ]


def create_spark_persistence() -> SparkAuditPersistence:
    """
    Create the Spark-backed persistence adapter lazily.

    PySpark is imported here rather than at module import time so commands
    such as `python job.py --help` and local unit tests do not require
    PySpark merely to import job.py.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    return SparkAuditPersistence(spark)


def count_findings_by_severity(
    findings,
    severity: Severity,
) -> int:
    return sum(finding.severity == severity for finding in findings)


def main():
    args = parse_args()

    run_id = str(uuid4())

    started_at = datetime.now(timezone.utc)

    environments = load_environments()

    print(f"Loaded {len(environments)} environments")

    result = fetch_catalog_inventory(environments)

    inventory = result.inventory

    all_findings = run_rules(inventory) if not inventory.is_empty() else []

    persistence = create_spark_persistence()

    persistence.ensure_tables()

    previous_run = persistence.load_previous_complete_run()

    previous_findings = []

    if previous_run is not None:
        previous_records = persistence.load_findings_for_run(previous_run.run_id)

        previous_findings = current_findings_from_records(previous_records)

    finding_deltas = compare_findings(
        current_findings=all_findings,
        previous_findings=previous_findings,
        current_scan_complete=(result.status == "COMPLETE"),
    )

    completed_at = datetime.now(timezone.utc)

    finding_records = build_finding_records(
        run_id=run_id,
        deltas=finding_deltas,
        observed_at=completed_at,
    )

    failed_environments = tuple(failure.environment for failure in result.failures)

    run_record = AuditRunRecord(
        run_id=run_id,
        started_at=started_at,
        completed_at=completed_at,
        scan_status=result.status,
        configured_environment_count=(result.configured_count),
        successful_environment_count=len(result.successful_environments),
        failed_environment_count=len(result.failures),
        catalog_count=inventory.height,
        high_count=count_findings_by_severity(
            all_findings,
            Severity.HIGH,
        ),
        warning_count=count_findings_by_severity(
            all_findings,
            Severity.WARNING,
        ),
        recommendation_count=(
            count_findings_by_severity(
                all_findings,
                Severity.RECOMMENDATION,
            )
        ),
        total_finding_count=len(all_findings),
        failed_environments=(failed_environments),
    )

    # Write findings first and the run summary last.
    #
    # The run row acts as the completion marker. If writing findings fails,
    # no COMPLETE run row is written and a future execution cannot
    # accidentally use an incomplete run as its comparison baseline.
    persistence.save_findings(finding_records)

    persistence.save_run(run_record)

    report_findings = filter_findings_by_rule(
        all_findings,
        args.rule,
    )

    print_terminal_report(
        result,
        report_findings,
        verbose=args.verbose,
    )

    export_csv_reports(
        inventory,
        report_findings,
    )

    export_markdown_report(
        result,
        report_findings,
    )

    print_report_files()


if __name__ == "__main__":
    main()
