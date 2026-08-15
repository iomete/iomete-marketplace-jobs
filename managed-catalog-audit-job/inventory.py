import json
import urllib.request
from datetime import datetime, timezone

import polars as pl

from config import Environment
from models import InventoryResult, ScanFailure

CATALOGS_PATH = "/api/v1/admin/spark/settings/catalogs"


def get_json(base_url: str, path: str, token: str):
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        headers={"Authorization": f"Bearer {token}"},
    )

    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read())


def catalogs_to_dataframe(data):
    if isinstance(data, str):
        data = json.loads(data)

    rows = []

    for item in data.get("items", []):
        row = {}

        for key, value in item.items():
            if isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    row[f"{key}_{nested_key}"] = nested_value

            elif isinstance(value, list):
                for entry in value:
                    if isinstance(entry, dict) and "key" in entry and "value" in entry:
                        row[f"{key}_{entry['key']}"] = entry["value"]

            else:
                row[key] = value

        rows.append(row)

    return pl.DataFrame(
        rows,
        infer_schema_length=len(rows) if rows else None,
    )


def fetch_catalog_inventory(
    environments: list[Environment],
) -> InventoryResult:
    dataframes = []
    successful_environments = []
    failures = []

    for environment in environments:
        try:
            response = get_json(
                environment.uri,
                CATALOGS_PATH,
                environment.token,
            )

            dataframe = catalogs_to_dataframe(response)

            successful_environments.append(environment.name)

            if dataframe.is_empty():
                print(f"✓ {environment.name}: 0 catalogs")
                continue

            dataframe = dataframe.with_columns(
                pl.lit(environment.name).alias("env_name")
            )

            dataframes.append(dataframe)

            print(f"✓ {environment.name}: " f"{len(dataframe)} catalogs")

        except Exception as error:
            failures.append(
                ScanFailure(
                    environment=environment.name,
                    error_type=type(error).__name__,
                    message=str(error),
                )
            )

            print(f"✗ {environment.name}: " f"ERROR - {error}")

    if dataframes:
        inventory = pl.concat(
            dataframes,
            how="diagonal",
        ).with_columns(pl.lit(datetime.now(timezone.utc)).alias("fetch_time"))
    else:
        inventory = pl.DataFrame()

    return InventoryResult(
        inventory=inventory,
        configured_count=len(environments),
        successful_environments=tuple(successful_environments),
        failures=tuple(failures),
    )
