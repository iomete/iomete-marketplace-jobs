from config import load_environments
from inventory import fetch_catalog_inventory
from reporting import (
    export_csv_reports,
    print_terminal_report,
)
from rules import run_rules


def main():
    environments = load_environments()

    print(f"Loaded {len(environments)} environments")

    inventory = fetch_catalog_inventory(environments)

    if inventory.is_empty():
        print("No catalog inventory collected.")
        return

    findings = run_rules(inventory)

    print_terminal_report(
        inventory,
        findings,
    )

    export_csv_reports(
        inventory,
        findings,
    )


if __name__ == "__main__":
    main()
