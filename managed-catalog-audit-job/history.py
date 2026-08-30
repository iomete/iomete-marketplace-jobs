from dataclasses import dataclass
from enum import Enum
import hashlib
import json

from models import Finding


class FindingStatus(str, Enum):
    NEW = "NEW"
    ONGOING = "ONGOING"
    RESOLVED = "RESOLVED"


@dataclass(frozen=True)
class FindingDelta:
    fingerprint: str
    status: FindingStatus
    finding: Finding


def _normalize_storage(
    storage: str | None,
) -> str | None:
    if storage is None:
        return None

    value = storage.strip()

    if not value:
        return None

    return value.rstrip("/")


def _sorted_values(
    values: tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(sorted(set(values)))


def _identity_for_finding(
    finding: Finding,
) -> dict:
    """
    Return the stable identity fields for a finding.

    The fingerprint deliberately excludes descriptive fields such as:
      - title
      - details
      - evidence
      - impact
      - recommendation

    Those can change as reporting improves without making an existing
    finding look like a brand-new problem.
    """

    if finding.rule_id == "MC001":
        # Same catalog still has an ownership conflict even if the
        # exact manager set changes between runs.
        return {
            "rule_id": finding.rule_id,
            "catalog": finding.catalog,
        }

    if finding.rule_id == "MC002":
        # URI inconsistency belongs to a catalog configuration in a
        # specific managing environment.
        return {
            "rule_id": finding.rule_id,
            "catalog": finding.catalog,
            "managers": _sorted_values(finding.managers),
        }

    if finding.rule_id == "MC003":
        # Credential variation belongs to the logical catalog/storage
        # combination. Credential groups and consumers may change while
        # the underlying finding remains ongoing.
        return {
            "rule_id": finding.rule_id,
            "catalog": finding.catalog,
            "storage": _normalize_storage(finding.storage),
        }

    if finding.rule_id == "MC004":
        # External resolution is specific to a consumer environment's
        # external catalog.
        return {
            "rule_id": finding.rule_id,
            "catalog": finding.catalog,
            "storage": _normalize_storage(finding.storage),
            "consumers": _sorted_values(finding.consumers),
        }

    # Safe fallback for future rules.
    return {
        "rule_id": finding.rule_id,
        "catalog": finding.catalog,
        "storage": _normalize_storage(finding.storage),
        "managers": _sorted_values(finding.managers),
        "consumers": _sorted_values(finding.consumers),
    }


def finding_fingerprint(
    finding: Finding,
) -> str:
    """
    Generate a stable SHA-256 fingerprint for a finding.

    Equivalent findings across weekly runs should receive the same
    fingerprint even if wording, evidence, or recommendations change.
    """
    identity = _identity_for_finding(finding)

    canonical = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _index_findings(
    findings: list[Finding],
) -> dict[str, Finding]:
    indexed = {}

    for finding in findings:
        fingerprint = finding_fingerprint(finding)

        if fingerprint in indexed:
            raise ValueError(
                "Duplicate finding identity detected for "
                f"{finding.rule_id}: {finding.catalog}"
            )

        indexed[fingerprint] = finding

    return indexed


def compare_findings(
    current_findings: list[Finding],
    previous_findings: list[Finding],
    current_scan_complete: bool = True,
) -> list[FindingDelta]:
    """
    Compare the current audit with the previous successful audit.

    Current + not previously present:
        NEW

    Current + previously present:
        ONGOING

    Previously present + no longer current:
        RESOLVED, but only when the current scan is complete.

    A partial scan cannot prove that a previous finding disappeared, so
    missing previous findings are not marked RESOLVED in that case.
    """
    current = _index_findings(current_findings)
    previous = _index_findings(previous_findings)

    deltas = []

    for fingerprint, finding in current.items():
        if fingerprint in previous:
            status = FindingStatus.ONGOING
        else:
            status = FindingStatus.NEW

        deltas.append(
            FindingDelta(
                fingerprint=fingerprint,
                status=status,
                finding=finding,
            )
        )

    if current_scan_complete:
        for fingerprint, finding in previous.items():
            if fingerprint in current:
                continue

            deltas.append(
                FindingDelta(
                    fingerprint=fingerprint,
                    status=FindingStatus.RESOLVED,
                    finding=finding,
                )
            )

    return sorted(
        deltas,
        key=lambda delta: (
            delta.status.value,
            delta.finding.rule_id,
            delta.finding.catalog or "",
            delta.fingerprint,
        ),
    )


def count_by_status(
    deltas: list[FindingDelta],
) -> dict[FindingStatus, int]:
    return {
        status: sum(1 for delta in deltas if delta.status == status)
        for status in FindingStatus
    }
