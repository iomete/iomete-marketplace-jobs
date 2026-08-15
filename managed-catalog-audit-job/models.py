from dataclasses import dataclass
from enum import Enum


class Severity(str, Enum):
    HIGH = "HIGH"
    WARNING = "WARNING"
    RECOMMENDATION = "RECOMMENDATION"


@dataclass(frozen=True)
class Finding:
    rule_id: str
    severity: Severity
    title: str
    catalog: str | None = None
    storage: str | None = None
    managers: tuple[str, ...] = ()
    consumers: tuple[str, ...] = ()
    details: str = ""
    recommendation: str = ""
