from dataclasses import dataclass
from enum import Enum

import polars as pl


class Severity(str, Enum):
    HIGH = "HIGH"
    WARNING = "WARNING"
    RECOMMENDATION = "RECOMMENDATION"


@dataclass(frozen=True)
class ScanFailure:
    environment: str
    error_type: str
    message: str


@dataclass(frozen=True)
class InventoryResult:
    inventory: pl.DataFrame
    configured_count: int
    successful_environments: tuple[str, ...] = ()
    failures: tuple[ScanFailure, ...] = ()

    @property
    def status(self) -> str:
        return "COMPLETE" if not self.failures else "PARTIAL"


@dataclass(frozen=True)
class Finding:
    rule_id: str
    severity: Severity
    title: str
    catalog: str | None = None
    storage: str | None = None
    managers: tuple[str, ...] = ()
    consumers: tuple[str, ...] = ()
    evidence: tuple[str, ...] = ()
    details: str = ""
    impact: str = ""
    recommendation: str = ""
