from dataclasses import asdict

import polars as pl

from models import Finding, InventoryResult, Severity

DEFAULT_FINDINGS_FILE = "managed_catalog_findings.csv"
DEFAULT_INVENTORY_FILE = "managed_catalog_inventory.csv"
DEFAULT_MARKDOWN_FILE = "managed_catalog_audit.md"


RULE_LABELS = {
    "MC001": "Catalog ownership conflicts",
    "MC002": "Internal catalog URI inconsistencies",
    "MC003": "Storage credential recommendations",
}


RULE_DESCRIPTIONS = {
    "MC001": (
        "Detects the same catalog name configured as internal in more than "
        "one IOMETE environment."
    ),
    "MC002": (
        "Validates that internal Iceberg REST catalogs have a usable "
        "properties_uri and that the URI points to "
        "'/internal/catalogs/<catalog-name>' using the same catalog "
        "name as the configuration."
    ),
    "MC003": (
        "Detects the same catalog and storage configuration using multiple "
        "object-storage access keys. This is supported and reported as a "
        "recommendation rather than a failure."
    ),
}


def _severity_counts(
    findings: list[Finding],
) -> dict[Severity, int]:
    return {
        severity: sum(1 for finding in findings if finding.severity == severity)
        for severity in Severity
    }


def _rule_counts(
    findings: list[Finding],
) -> dict[str, int]:
    counts = {}

    for finding in findings:
        counts[finding.rule_id] = counts.get(finding.rule_id, 0) + 1

    return counts


def print_terminal_report(
    result: InventoryResult,
    findings: list[Finding],
    verbose: bool = False,
):
    counts = _severity_counts(findings)
    rule_counts = _rule_counts(findings)

    print()
    print("=" * 70)
    print("MANAGED CATALOG AUDIT")
    print("=" * 70)

    print()
    print("Scan")
    print(f"  Configured environments : " f"{result.configured_count}")
    print(f"  Successful              : " f"{len(result.successful_environments)}")
    print(f"  Failed                  : " f"{len(result.failures)}")
    print(f"  Status                  : " f"{result.status}")
    print(f"  Catalogs discovered     : " f"{len(result.inventory)}")

    print()
    print("Findings")
    print(f"  HIGH                    : " f"{counts[Severity.HIGH]}")
    print(f"  WARNING                 : " f"{counts[Severity.WARNING]}")
    print(f"  RECOMMENDATION          : " f"{counts[Severity.RECOMMENDATION]}")
    print(f"  TOTAL                   : " f"{len(findings)}")

    print()
    print("Findings by rule")

    if rule_counts:
        for rule_id in sorted(rule_counts):
            label = RULE_LABELS.get(
                rule_id,
                "Unknown rule",
            )

            print(f"  {rule_id} " f"{label:<38} : " f"{rule_counts[rule_id]}")
    else:
        print("  None")

    if result.failures:
        print()
        print("Failed environments")

        for failure in result.failures:
            print(
                f"  {failure.environment}: "
                f"{failure.error_type} - "
                f"{failure.message}"
            )

    if verbose:
        _print_detailed_findings(findings)

    print("=" * 70)


def _print_detailed_findings(
    findings: list[Finding],
):
    print()
    print("DETAILED FINDINGS")
    print("=" * 70)

    if not findings:
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
            print(f"[{finding.rule_id}] " f"{finding.title}")

            if finding.catalog:
                print(f"Catalog        : " f"{finding.catalog}")

            if finding.storage:
                print(f"Storage        : " f"{finding.storage}")

            if finding.managers:
                print("Managers       : " + ", ".join(finding.managers))

            if finding.consumers:
                print("Environments   : " + ", ".join(finding.consumers))

            if finding.details:
                print(f"What detected  : " f"{finding.details}")

            if finding.evidence:
                print("Evidence       :")

                for evidence in finding.evidence:
                    print(f"  - {evidence}")

            if finding.impact:
                print(f"Why it matters : " f"{finding.impact}")

            if finding.recommendation:
                print(f"Recommendation : " f"{finding.recommendation}")

            print()


def _finding_rows(
    findings: list[Finding],
):
    rows = []

    for finding in findings:
        row = asdict(finding)

        row["severity"] = finding.severity.value
        row["managers"] = ", ".join(finding.managers)
        row["consumers"] = ", ".join(finding.consumers)
        row["evidence"] = " | ".join(finding.evidence)

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
        "evidence",
        "details",
        "impact",
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


def export_markdown_report(
    result: InventoryResult,
    findings: list[Finding],
    output_file=DEFAULT_MARKDOWN_FILE,
):
    counts = _severity_counts(findings)
    rule_counts = _rule_counts(findings)

    lines = [
        "# Managed Catalog Audit",
        "",
        "## Executive Summary",
        "",
        f"- Scan status: **{result.status}**",
        (f"- Configured environments: " f"{result.configured_count}"),
        (f"- Successful environments: " f"{len(result.successful_environments)}"),
        (f"- Failed environments: " f"{len(result.failures)}"),
        (f"- Catalogs discovered: " f"{len(result.inventory)}"),
        f"- Total findings: {len(findings)}",
        (f"- HIGH: " f"{counts[Severity.HIGH]}"),
        (f"- WARNING: " f"{counts[Severity.WARNING]}"),
        (f"- RECOMMENDATION: " f"{counts[Severity.RECOMMENDATION]}"),
        "",
        "## Audit Rules",
        "",
    ]

    for rule_id in sorted(RULE_DESCRIPTIONS):
        label = RULE_LABELS[rule_id]
        description = RULE_DESCRIPTIONS[rule_id]

        lines.extend(
            [
                (f"### {rule_id} - " f"{label}"),
                "",
                description,
                "",
            ]
        )

    lines.extend(
        [
            "## Findings by Rule",
            "",
        ]
    )

    if rule_counts:
        for rule_id in sorted(rule_counts):
            label = RULE_LABELS.get(
                rule_id,
                "Unknown rule",
            )

            lines.append(f"- **{rule_id}** - " f"{label}: " f"{rule_counts[rule_id]}")
    else:
        lines.append("- No findings detected.")

    if result.failures:
        lines.extend(
            [
                "",
                "## Failed Environments",
                "",
            ]
        )

        for failure in result.failures:
            lines.append(
                f"- **{failure.environment}**: "
                f"{failure.error_type} - "
                f"{failure.message}"
            )

    lines.extend(
        [
            "",
            "## Detailed Findings",
            "",
        ]
    )

    if not findings:
        lines.append("No findings detected.")

    for finding in findings:
        lines.extend(
            [
                (f"### {finding.rule_id} - " f"{finding.title}"),
                "",
                (f"**Severity:** " f"{finding.severity.value}"),
                "",
            ]
        )

        if finding.catalog:
            lines.extend(
                [
                    (f"**Catalog:** " f"`{finding.catalog}`"),
                    "",
                ]
            )

        if finding.storage:
            lines.extend(
                [
                    (f"**Storage:** " f"`{finding.storage}`"),
                    "",
                ]
            )

        if finding.managers:
            lines.extend(
                [
                    (f"**Managers:** " f"{', '.join(finding.managers)}"),
                    "",
                ]
            )

        if finding.consumers:
            lines.extend(
                [
                    (f"**Environments:** " f"{', '.join(finding.consumers)}"),
                    "",
                ]
            )

        if finding.details:
            lines.extend(
                [
                    "**What was detected**",
                    "",
                    finding.details,
                    "",
                ]
            )

        if finding.evidence:
            lines.extend(
                [
                    "**Evidence**",
                    "",
                ]
            )

            for evidence in finding.evidence:
                lines.append(f"- {evidence}")

            lines.append("")

        if finding.impact:
            lines.extend(
                [
                    "**Why it matters**",
                    "",
                    finding.impact,
                    "",
                ]
            )

        if finding.recommendation:
            lines.extend(
                [
                    "**Recommended action**",
                    "",
                    finding.recommendation,
                    "",
                ]
            )

        lines.extend(
            [
                "---",
                "",
            ]
        )

    with open(
        output_file,
        "w",
        encoding="utf-8",
    ) as file:
        file.write("\n".join(lines))


def print_report_files():
    print()
    print("Reports")
    print(f"  Findings CSV : " f"{DEFAULT_FINDINGS_FILE}")
    print(f"  Inventory CSV: " f"{DEFAULT_INVENTORY_FILE}")
    print(f"  Markdown     : " f"{DEFAULT_MARKDOWN_FILE}")
