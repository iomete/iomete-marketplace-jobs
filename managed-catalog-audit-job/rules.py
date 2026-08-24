from urllib.parse import unquote, urlparse

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


def _internal_catalogs(
    inventory: pl.DataFrame,
) -> pl.DataFrame:
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


# ---------------------------------------------------------------------------
# MC001
# ---------------------------------------------------------------------------


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
                    "A managed catalog should normally have one "
                    "authoritative IOMETE manager. Multiple internal "
                    "managers make catalog ownership ambiguous."
                ),
                recommendation=(
                    "Confirm which IOMETE environment is the "
                    "authoritative manager for this catalog. Keep the "
                    "catalog internal on that environment and configure "
                    "other environments that need access to consume it "
                    "as an external catalog."
                ),
            )
        )

    return findings


# ---------------------------------------------------------------------------
# MC002
# ---------------------------------------------------------------------------


def _internal_uri_error(
    catalog: str,
    uri: object,
) -> str | None:
    """
    Validate the structural relationship between an internal
    REST catalog and its configured properties_uri.

    Return an error message when inconsistent.
    Return None when valid.
    """
    if not isinstance(uri, str) or not uri.strip():
        return "Internal REST catalog has no properties_uri."

    uri = uri.strip()
    parsed = urlparse(uri)

    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return (
            "Internal REST catalog has a malformed properties_uri. "
            "Expected an HTTP(S) URI."
        )

    path = parsed.path.rstrip("/")

    segments = [unquote(segment) for segment in path.split("/") if segment]

    if len(segments) < 3:
        return (
            "Internal REST catalog URI does not contain the expected "
            "'/internal/catalogs/<catalog-name>' path."
        )

    scope, resource, uri_catalog = segments[-3:]

    if scope != "internal" or resource != "catalogs":
        return (
            "Internal catalog URI does not point to an " "'/internal/catalogs/' path."
        )

    if uri_catalog != catalog:
        return (
            f"Catalog name mismatch: configuration name is '{catalog}' "
            f"but properties_uri points to '{uri_catalog}'."
        )

    return None


def find_internal_uri_inconsistency(
    inventory: pl.DataFrame,
) -> list[Finding]:
    """
    MC002: Internal REST catalog URI consistency.

    Validate that internal Iceberg REST catalogs have a valid
    properties_uri whose catalog path matches the configured
    catalog name.

    Shared lakehouseDir values are deliberately not evaluated
    by this rule because real environment data showed that
    multiple legitimate catalogs can share the same base bucket.
    """
    required_columns = {
        "name",
        "catalogType_type",
        "catalogType_subtype",
        "catalogType_classification",
        "properties_uri",
        "env_name",
    }

    _require_columns(
        inventory,
        "MC002",
        required_columns,
    )

    candidates = inventory.filter(
        (pl.col("catalogType_classification") == "internal")
        & (pl.col("catalogType_type") == "iceberg")
        & (pl.col("catalogType_subtype") == "rest")
        & pl.col("name").is_not_null()
        & ~pl.col("name").is_in(list(SYSTEM_CATALOGS))
    ).select(
        [
            "env_name",
            "name",
            "properties_uri",
        ]
    )
    findings = []

    for row in candidates.iter_rows(named=True):
        catalog = row["name"]
        environment = row["env_name"]
        uri = row["properties_uri"]

        error = _internal_uri_error(
            catalog,
            uri,
        )

        if error is None:
            continue

        findings.append(
            Finding(
                rule_id="MC002",
                severity=Severity.HIGH,
                title="Internal REST catalog URI is inconsistent",
                catalog=catalog,
                managers=(environment,),
                evidence=(f"{environment} / {catalog} / " f"properties_uri={uri!r}",),
                details=error,
                impact=(
                    "An inconsistent internal REST catalog URI can "
                    "cause the catalog configuration to reference the "
                    "wrong catalog endpoint or prevent the catalog from "
                    "being resolved correctly."
                ),
                recommendation=(
                    "Review the internal catalog configuration and "
                    "ensure properties_uri points to the expected "
                    "'/internal/catalogs/<catalog-name>' endpoint for "
                    "this catalog."
                ),
            )
        )

    return findings


# ---------------------------------------------------------------------------
# MC003
# ---------------------------------------------------------------------------


def find_access_key_variation(
    inventory: pl.DataFrame,
) -> list[Finding]:
    """
    MC003: Storage credential consistency.

    Detect the same catalog/storage using multiple
    object-storage access keys.

    This configuration is supported, so findings are
    recommendations rather than failures.
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
                    "This configuration is supported and is not "
                    "considered a failure. Using multiple credentials "
                    "can make credential management and rotation more "
                    "complex."
                ),
                recommendation=(
                    "No immediate remediation is required. Where "
                    "practical, standardize the storage credential "
                    "used by environments accessing the same catalog."
                ),
            )
        )

    return findings


# ---------------------------------------------------------------------------
# MC004
# ---------------------------------------------------------------------------


def _external_target(
    uri: object,
) -> tuple[str | None, str | None]:
    """
    Extract the target catalog name from an external REST catalog URI.

    Example:

        https://cp.example.com/catalogs/finance
                                      ^^^^^^^
                                      target

    Returns:
        (target_name, None) when the URI can be parsed.
        (None, error_message) when it cannot.
    """
    if not isinstance(uri, str) or not uri.strip():
        return (
            None,
            "External REST catalog has no properties_uri.",
        )

    parsed = urlparse(uri.strip())

    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return (
            None,
            (
                "External REST catalog has a malformed "
                "properties_uri. Expected an HTTP(S) URI."
            ),
        )

    segments = [
        unquote(segment) for segment in parsed.path.rstrip("/").split("/") if segment
    ]

    if len(segments) < 2 or segments[-2] != "catalogs":
        return (
            None,
            (
                "External REST catalog URI does not contain the "
                "expected '/catalogs/<catalog-name>' path."
            ),
        )

    return segments[-1], None


def find_unresolved_external_catalogs(
    inventory: pl.DataFrame,
) -> list[Finding]:
    """
    MC004: External managed catalog target validation.

    Validate that an external Iceberg REST catalog can be mapped
    back to an internal Iceberg REST catalog discovered somewhere
    in the complete scanned inventory.

    Resolution uses two pieces of information:

        1. Target catalog name parsed from properties_uri.
        2. lakehouseDir.

    Example:

        External configuration:
            env_name       = a2
            name           = finance_alias
            properties_uri = https://cp.example/catalogs/finance
            lakehouseDir   = s3://finance-bucket/

        Internal configuration:
            env_name       = a3
            name           = finance
            lakehouseDir   = s3://finance-bucket/

    This is considered resolved even though the local external
    alias "finance_alias" differs from the target name "finance".

    Important:

    - The external catalog's local name does NOT need to match
      the target catalog name.
    - JDBC catalogs are outside the scope of this rule.
    - Non-Iceberg catalogs are outside the scope.
    - MC001 separately handles cases where several environments
      internally manage the same catalog.
    """
    required_columns = {
        "name",
        "lakehouseDir",
        "catalogType_type",
        "catalogType_subtype",
        "catalogType_classification",
        "properties_uri",
        "env_name",
    }

    _require_columns(
        inventory,
        "MC004",
        required_columns,
    )

    external = inventory.filter(
        (pl.col("catalogType_classification") == "external")
        & (pl.col("catalogType_type") == "iceberg")
        & (pl.col("catalogType_subtype") == "rest")
        & pl.col("name").is_not_null()
        & ~pl.col("name").is_in(list(SYSTEM_CATALOGS))
    )

    internal = inventory.filter(
        (pl.col("catalogType_classification") == "internal")
        & (pl.col("catalogType_type") == "iceberg")
        & (pl.col("catalogType_subtype") == "rest")
        & pl.col("name").is_not_null()
        & ~pl.col("name").is_in(list(SYSTEM_CATALOGS))
    ).select(
        [
            "env_name",
            "name",
            "lakehouseDir",
        ]
    )

    findings = []

    for row in external.iter_rows(named=True):
        consumer = row["env_name"]
        external_name = row["name"]
        storage = row["lakehouseDir"]
        uri = row["properties_uri"]

        target_name, uri_error = _external_target(uri)

        # ---------------------------------------------------------------
        # Case 1:
        # The external URI itself cannot tell us what catalog it targets.
        # ---------------------------------------------------------------

        if uri_error is not None:
            findings.append(
                Finding(
                    rule_id="MC004",
                    severity=Severity.HIGH,
                    title=("External REST catalog target " "cannot be resolved"),
                    catalog=external_name,
                    storage=storage,
                    consumers=(consumer,),
                    evidence=(
                        f"{consumer} / {external_name} / " f"properties_uri={uri!r}",
                    ),
                    details=uri_error,
                    impact=(
                        "The external catalog does not provide a "
                        "usable REST catalog target, so the audit "
                        "cannot verify that it references a managed "
                        "catalog present in the scanned IOMETE "
                        "environments."
                    ),
                    recommendation=(
                        "Review properties_uri and ensure it points "
                        "to the expected "
                        "'/catalogs/<catalog-name>' endpoint."
                    ),
                )
            )

            continue

        # ---------------------------------------------------------------
        # Find every internal catalog whose NAME matches the target
        # extracted from the external URI.
        # ---------------------------------------------------------------

        target_candidates = internal.filter(pl.col("name") == target_name)

        # ---------------------------------------------------------------
        # Then determine whether one of those candidates also has the
        # same storage location.
        #
        # We deliberately do NOT resolve solely by storage because the
        # real inventory showed that legitimate catalogs may share a
        # common base lakehouse directory.
        # ---------------------------------------------------------------

        if storage is None:
            storage_matches = target_candidates.head(0)
        else:
            storage_matches = target_candidates.filter(
                pl.col("lakehouseDir") == storage
            )

        # ---------------------------------------------------------------
        # Happy path.
        #
        # URI target exists AND storage agrees.
        # ---------------------------------------------------------------

        if not storage_matches.is_empty():
            continue

        # ---------------------------------------------------------------
        # Case 2:
        # Nothing internal has the catalog name referenced by the URI.
        # ---------------------------------------------------------------

        if target_candidates.is_empty():
            details = (
                f"External catalog '{external_name}' on "
                f"'{consumer}' points to target catalog "
                f"'{target_name}', but no internal Iceberg REST "
                "catalog with that name was found in the scanned "
                "inventory."
            )

            evidence = (
                f"Consumer: {consumer}",
                f"External catalog: {external_name}",
                f"URI target: {target_name}",
                f"Storage: {storage!r}",
                f"properties_uri: {uri}",
            )

            recommendation = (
                "Confirm that the target managed catalog exists "
                "in one of the environments included in the audit "
                "and that the external catalog points to the "
                "intended target."
            )

        # ---------------------------------------------------------------
        # Case 3:
        # Catalog name exists internally, but storage does not agree.
        # ---------------------------------------------------------------

        else:
            candidate_evidence = tuple(
                (
                    f"Internal candidate: "
                    f"{candidate['env_name']} / "
                    f"{candidate['name']} / "
                    f"storage={candidate['lakehouseDir']!r}"
                )
                for candidate in target_candidates.select(
                    "env_name",
                    "name",
                    "lakehouseDir",
                ).iter_rows(named=True)
            )

            details = (
                f"External catalog '{external_name}' on "
                f"'{consumer}' points to target catalog "
                f"'{target_name}', and internal catalogs with "
                "that name exist, but none use the external "
                f"catalog's configured storage {storage!r}."
            )

            evidence = (
                f"Consumer: {consumer}",
                f"External catalog: {external_name}",
                f"URI target: {target_name}",
                f"External storage: {storage!r}",
                *candidate_evidence,
            )

            recommendation = (
                "Verify that the external catalog points to the "
                "intended managed catalog and that its "
                "lakehouseDir matches the authoritative internal "
                "catalog."
            )

        findings.append(
            Finding(
                rule_id="MC004",
                severity=Severity.HIGH,
                title=("External REST catalog target " "cannot be resolved"),
                catalog=external_name,
                storage=storage,
                consumers=(consumer,),
                evidence=evidence,
                details=details,
                impact=(
                    "An external managed catalog should resolve "
                    "to an authoritative internal catalog in the "
                    "scanned IOMETE topology. An unresolved target "
                    "can indicate a stale, incorrect, or incomplete "
                    "cross-environment catalog configuration."
                ),
                recommendation=recommendation,
            )
        )

    return findings


# ---------------------------------------------------------------------------
# Run all rules
# ---------------------------------------------------------------------------


def run_rules(
    inventory: pl.DataFrame,
) -> list[Finding]:
    findings = []

    findings.extend(find_multiple_managers(inventory))

    findings.extend(find_internal_uri_inconsistency(inventory))

    findings.extend(find_access_key_variation(inventory))

    findings.extend(find_unresolved_external_catalogs(inventory))

    return findings
