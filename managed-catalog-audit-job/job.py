import argparse

from config import load_environments
from inventory import fetch_catalog_inventory
from reporting import (
    export_csv_reports,
    export_markdown_report,
    print_report_files,
    print_terminal_report,
)
from rules import run_rules


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Audit managed catalog topology and "
            "configuration across IOMETE environments."
        )
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help=(
            "Print all detailed findings to the terminal. "
            "Detailed findings are always written to the "
            "CSV and Markdown reports."
        ),
    )

    return parser.parse_args()


def main():
    args = parse_args()

    environments = load_environments()

    print(f"Loaded {len(environments)} environments")

    result = fetch_catalog_inventory(environments)

    inventory = result.inventory

    findings = run_rules(inventory) if not inventory.is_empty() else []

    print_terminal_report(
        result,
        findings,
        verbose=args.verbose,
    )

    export_csv_reports(
        inventory,
        findings,
    )

    export_markdown_report(
        result,
        findings,
    )

    print_report_files()


if __name__ == "__main__":
    main()
