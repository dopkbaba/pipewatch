"""Core health check data model and status evaluation for ETL pipelines."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class HealthStatus(Enum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class PipelineHealth:
    """Represents the health snapshot of a single ETL pipeline."""

    name: str
    last_run_at: Optional[datetime] = None
    last_run_duration_seconds: Optional[float] = None
    records_processed: Optional[int] = None
    error_count: int = 0
    tags: dict = field(default_factory=dict)

    def evaluate(
        self,
        max_error_count: int = 0,
        max_duration_seconds: Optional[float] = None,
        stale_after_seconds: Optional[float] = None,
    ) -> HealthStatus:
        """Evaluate pipeline health against provided thresholds."""
        if self.last_run_at is None:
            return HealthStatus.UNKNOWN

        now = datetime.utcnow()
        age_seconds = (now - self.last_run_at).total_seconds()

        if stale_after_seconds is not None and age_seconds > stale_after_seconds:
            return HealthStatus.CRITICAL

        if self.error_count > max_error_count:
            return HealthStatus.CRITICAL

        if (
            max_duration_seconds is not None
            and self.last_run_duration_seconds is not None
            and self.last_run_duration_seconds > max_duration_seconds
        ):
            return HealthStatus.WARNING

        return HealthStatus.OK

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "last_run_duration_seconds": self.last_run_duration_seconds,
            "records_processed": self.records_processed,
            "error_count": self.error_count,
            "tags": self.tags,
        }
