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
                evidence=tuple(
                    f"{manager} / {catalog} / internal" for manager in managers
                ),
                details=(
                    f"Catalog '{catalog}' is configured as internal on "
                    f"{len(managers)} environments: "
                    f"{', '.join(managers)}."
                ),
                impact=(
                    "A managed catalog should normally have one authoritative "
                    "IOMETE manager. Multiple internal managers make catalog "
                    "ownership ambiguous."
                ),
                recommendation=(
                    "Confirm which IOMETE environment is the authoritative "
                    "manager for this catalog. Keep the catalog internal on "
                    "that environment and configure other environments that "
                    "need access to consume it as an external catalog."
                ),
            )
        )

    return findings


def find_shared_managed_storage(
    inventory: pl.DataFrame,
) -> list[Finding]:
    """
    MC002: Storage ownership conflict.

    Detect different internal catalogs that manage the
    same underlying lakehouse storage location.
    """
    internal = _internal_catalogs(inventory)

    _require_columns(
        inventory,
        "MC002",
        {"lakehouseDir"},
    )

    if internal.is_empty():
        return []

    candidates = internal.filter(
        pl.col("lakehouseDir").is_not_null() & (pl.col("lakehouseDir") != "")
    )

    grouped = (
        candidates.group_by("lakehouseDir")
        .agg(
            pl.col("env_name").unique().sort().alias("managers"),
            pl.col("name").unique().sort().alias("catalogs"),
        )
        .filter(
            (pl.col("managers").list.len() > 1) & (pl.col("catalogs").list.len() > 1)
        )
    )

    findings = []

    for row in grouped.iter_rows(named=True):
        managers = tuple(row["managers"])
        catalogs = tuple(row["catalogs"])
        storage = row["lakehouseDir"]

        evidence_rows = candidates.filter(pl.col("lakehouseDir") == storage).select(
            "env_name",
            "name",
        )

        evidence = tuple(
            f"{item['env_name']} / {item['name']} / internal"
            for item in evidence_rows.iter_rows(named=True)
        )

        findings.append(
            Finding(
                rule_id="MC002",
                severity=Severity.HIGH,
                title=("Storage location is managed by " "different internal catalogs"),
                storage=storage,
                managers=managers,
                evidence=evidence,
                details=(
                    f"Storage '{storage}' is referenced by different internal "
                    f"catalogs across {len(managers)} environments. "
                    f"Catalogs: {', '.join(catalogs)}. "
                    f"Managers: {', '.join(managers)}."
                ),
                impact=(
                    "Different independently managed catalogs pointing to the "
                    "same lakehouse storage can create ambiguous storage "
                    "ownership and inconsistent catalog state."
                ),
                recommendation=(
                    "Determine which IOMETE environment should own this "
                    "storage-backed catalog. Keep one authoritative internal "
                    "manager and review the other internal configurations."
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

    findings = []

    for group in candidates.partition_by(
        [
            "name",
            "lakehouseDir",
            "credentials_endpoint",
        ],
        maintain_order=True,
    ):
        environments = sorted(group["env_name"].unique().to_list())

        if len(environments) <= 1:
            continue

        credential_groups = {}

        for row in group.select(
            "env_name",
            "credentials_accessKey",
        ).iter_rows(named=True):
            credential_groups.setdefault(
                row["credentials_accessKey"],
                set(),
            ).add(row["env_name"])

        if len(credential_groups) <= 1:
            continue

        catalog = group["name"][0]
        storage = group["lakehouseDir"][0]

        evidence = tuple(
            f"Credential {index}: " f"{', '.join(sorted(group_envs))}"
            for index, group_envs in enumerate(
                credential_groups.values(),
                start=1,
            )
        )

        findings.append(
            Finding(
                rule_id="MC003",
                severity=Severity.RECOMMENDATION,
                title=("Multiple storage access keys are used " "for the same catalog"),
                catalog=catalog,
                storage=storage,
                consumers=tuple(environments),
                evidence=evidence,
                details=(
                    f"Catalog '{catalog}' uses "
                    f"{len(credential_groups)} different storage "
                    f"access keys across {len(environments)} "
                    f"environments: {', '.join(environments)}."
                ),
                impact=(
                    "This configuration is supported and is not considered "
                    "a failure. Using multiple credentials can make credential "
                    "management and rotation more complex."
                ),
                recommendation=(
                    "No immediate remediation is required. Where practical, "
                    "standardize the storage credential used by environments "
                    "accessing the same catalog."
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
