from dataclasses import asdict

import polars as pl

from models import Finding, Severity

DEFAULT_FINDINGS_FILE = "managed_catalog_findings.csv"
DEFAULT_INVENTORY_FILE = "managed_catalog_inventory.csv"


def print_terminal_report(
    inventory: pl.DataFrame,
    findings: list[Finding],
):
    high = sum(1 for finding in findings if finding.severity == Severity.HIGH)

    warning = sum(1 for finding in findings if finding.severity == Severity.WARNING)

    recommendations = sum(
        1 for finding in findings if finding.severity == Severity.RECOMMENDATION
    )

    environments = (
        inventory["env_name"].n_unique() if "env_name" in inventory.columns else 0
    )

    print()
    print("=" * 70)
    print("MANAGED CATALOG AUDIT")
    print("=" * 70)
    print(f"Environments scanned : {environments}")
    print(f"Catalogs discovered  : {len(inventory)}")
    print(f"Findings             : {len(findings)}")
    print(f"High                 : {high}")
    print(f"Warning              : {warning}")
    print(f"Recommendations      : {recommendations}")
    print("=" * 70)

    if not findings:
        print()
        print("No findings detected.")
        return

    severity_order = [
        Severity.HIGH,
        Severity.WARNING,
        Severity.RECOMMENDATION,
    ]

    for severity in severity_order:
        matching = [finding for finding in findings if finding.severity == severity]

        if not matching:
            continue

        print()
        print(severity.value)
        print("-" * 70)

        for finding in matching:
            print(f"[{finding.rule_id}] {finding.title}")

            if finding.catalog:
                print(f"Catalog        : {finding.catalog}")

            if finding.storage:
                print(f"Storage        : {finding.storage}")

            if finding.managers:
                print("Managers       : " + ", ".join(finding.managers))

            if finding.consumers:
                print("Environments   : " + ", ".join(finding.consumers))

            if finding.details:
                print(f"Details        : {finding.details}")

            if finding.recommendation:
                print(f"Recommendation : {finding.recommendation}")

            print()


def _finding_rows(findings: list[Finding]):
    rows = []

    for finding in findings:
        row = asdict(finding)

        row["severity"] = finding.severity.value
        row["managers"] = ", ".join(finding.managers)
        row["consumers"] = ", ".join(finding.consumers)

        rows.append(row)

    return rows


def export_csv_reports(
    inventory: pl.DataFrame,
    findings: list[Finding],
    findings_file=DEFAULT_FINDINGS_FILE,
    inventory_file=DEFAULT_INVENTORY_FILE,
):
    finding_headers = [
        "rule_id",
        "severity",
        "title",
        "catalog",
        "storage",
        "managers",
        "consumers",
        "details",
        "recommendation",
    ]

    finding_rows = _finding_rows(findings)

    if finding_rows:
        findings_df = pl.DataFrame(finding_rows).select(finding_headers)
    else:
        findings_df = pl.DataFrame(
            schema={header: pl.String for header in finding_headers}
        )

    findings_df.write_csv(findings_file)
    inventory.write_csv(inventory_file)

    print()
    print(f"Findings report : {findings_file}")
    print(f"Inventory report: {inventory_file}")
