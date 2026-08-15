import polars as pl

from models import Finding, Severity

SYSTEM_CATALOGS = {"spark_catalog"}


def _require_columns(
    inventory: pl.DataFrame,
    rule_id: str,
    columns: set[str],
) -> None:
    missing = columns - set(inventory.columns)

    if missing:
        raise ValueError(
            f"{rule_id} cannot run because inventory is missing "
            f"required columns: {', '.join(sorted(missing))}"
        )


def _internal_catalogs(inventory: pl.DataFrame) -> pl.DataFrame:
    _require_columns(
        inventory,
        "internal catalog rules",
        {
            "name",
            "catalogType_classification",
            "env_name",
        },
    )

    internal = inventory.filter(pl.col("catalogType_classification") == "internal")

    return internal.filter(
        pl.col("name").is_not_null() & ~pl.col("name").is_in(list(SYSTEM_CATALOGS))
    )


def find_multiple_managers(
    inventory: pl.DataFrame,
) -> list[Finding]:
    """
    MC001: Catalog ownership conflict.

    Detect the same catalog being managed internally
    by more than one IOMETE environment.
    """
    internal = _internal_catalogs(inventory)

    if internal.is_empty():
        return []

    grouped = (
        internal.group_by("name")
        .agg(pl.col("env_name").unique().sort().alias("managers"))
        .filter(pl.col("managers").list.len() > 1)
    )

    findings = []

    for row in grouped.iter_rows(named=True):
        managers = tuple(row["managers"])
        catalog = row["name"]

        findings.append(
            Finding(
                rule_id="MC001",
                severity=Severity.HIGH,
                title="Catalog is managed internally by multiple instances",
                catalog=catalog,
                managers=managers,
                details=(
                    f"Catalog '{catalog}' is configured as internal on "
                    f"{len(managers)} environments: "
                    f"{', '.join(managers)}."
                ),
                recommendation=(
                    "Choose one managing instance and configure the other "
                    "instances to consume the catalog externally."
                ),
            )
        )

    return findings


def find_shared_managed_storage(
    inventory: pl.DataFrame,
) -> list[Finding]:
    """
    MC002: Storage ownership conflict.

    Detect the same lakehouse storage location being
    managed internally by multiple environments.
    """
    internal = _internal_catalogs(inventory)

    _require_columns(
        inventory,
        "MC002",
        {"lakehouseDir"},
    )

    if internal.is_empty():
        return []

    grouped = (
        internal.filter(
            pl.col("lakehouseDir").is_not_null() & (pl.col("lakehouseDir") != "")
        )
        .group_by("lakehouseDir")
        .agg(
            pl.col("env_name").unique().sort().alias("managers"),
            pl.col("name").unique().sort().alias("catalogs"),
        )
        .filter(pl.col("managers").list.len() > 1)
    )

    findings = []

    for row in grouped.iter_rows(named=True):
        managers = tuple(row["managers"])
        catalogs = tuple(row["catalogs"])
        storage = row["lakehouseDir"]

        findings.append(
            Finding(
                rule_id="MC002",
                severity=Severity.HIGH,
                title="Storage location is managed by multiple instances",
                storage=storage,
                managers=managers,
                details=(
                    f"Storage '{storage}' is managed internally from "
                    f"{len(managers)} environments: "
                    f"{', '.join(managers)}. "
                    f"Catalogs involved: {', '.join(catalogs)}."
                ),
                recommendation=(
                    "Verify the intended owner of this storage location "
                    "and keep only one internal manager where possible."
                ),
            )
        )

    return findings


def find_access_key_variation(
    inventory: pl.DataFrame,
) -> list[Finding]:
    """
    MC003: Storage credential consistency.

    Detect the same catalog/storage using multiple
    object-storage access keys.

    This is supported, but using one consistent access
    key is preferred.
    """
    required_columns = {
        "name",
        "lakehouseDir",
        "credentials_endpoint",
        "credentials_accessKey",
        "env_name",
    }

    _require_columns(
        inventory,
        "MC003",
        required_columns,
    )

    candidates = inventory.filter(
        pl.col("name").is_not_null()
        & pl.col("lakehouseDir").is_not_null()
        & pl.col("credentials_endpoint").is_not_null()
        & pl.col("credentials_accessKey").is_not_null()
        & ~pl.col("name").is_in(list(SYSTEM_CATALOGS))
    )

    grouped = (
        candidates.group_by(
            [
                "name",
                "lakehouseDir",
                "credentials_endpoint",
            ]
        )
        .agg(
            pl.col("env_name").unique().sort().alias("environments"),
            pl.col("credentials_accessKey").n_unique().alias("access_key_count"),
        )
        .filter(
            (pl.col("environments").list.len() > 1) & (pl.col("access_key_count") > 1)
        )
    )

    findings = []

    for row in grouped.iter_rows(named=True):
        environments = tuple(row["environments"])

        findings.append(
            Finding(
                rule_id="MC003",
                severity=Severity.RECOMMENDATION,
                title=("Multiple storage access keys are used " "for the same catalog"),
                catalog=row["name"],
                storage=row["lakehouseDir"],
                consumers=environments,
                details=(
                    f"Catalog '{row['name']}' uses "
                    f"{row['access_key_count']} different storage "
                    f"access keys across {len(environments)} "
                    f"environments: {', '.join(environments)}."
                ),
                recommendation=(
                    "This configuration is supported, but using one "
                    "consistent storage access key is preferred where "
                    "operationally possible."
                ),
            )
        )

    return findings


def run_rules(
    inventory: pl.DataFrame,
) -> list[Finding]:
    findings = []

    findings.extend(find_multiple_managers(inventory))

    findings.extend(find_shared_managed_storage(inventory))

    findings.extend(find_access_key_variation(inventory))

    return findings
