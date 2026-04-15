"""Base backend interface for pipeline metric sources."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class PipelineMetrics:
    """Raw metrics fetched from a backend for a single pipeline."""

    pipeline_id: str
    last_run: Optional[datetime] = None
    last_success: Optional[datetime] = None
    row_count: Optional[int] = None
    error_rate: Optional[float] = None
    duration_seconds: Optional[float] = None
    extra: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f"PipelineMetrics(pipeline_id={self.pipeline_id!r}, "
            f"last_run={self.last_run}, row_count={self.row_count})"
        )


class BackendBase(ABC):
    """Abstract base class that all pipewatch backends must implement."""

    @abstractmethod
    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        """Fetch current metrics for the given pipeline.

        Args:
            pipeline_id: Unique identifier for the pipeline.

        Returns:
            A PipelineMetrics instance populated with available data.

        Raises:
            BackendError: If the backend cannot retrieve metrics.
        """

    @abstractmethod
    def list_pipelines(self) -> list[str]:
        """Return a list of all known pipeline IDs available in this backend."""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


class BackendError(Exception):
    """Raised when a backend fails to fetch pipeline metrics."""
