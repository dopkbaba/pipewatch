"""In-memory backend for testing and local development."""

from datetime import datetime
from typing import Optional

from pipewatch.backends.base import BackendBase, BackendError, PipelineMetrics


class MemoryBackend(BackendBase):
    """A simple in-memory backend that stores metrics in a dict.

    Useful for unit tests and demos without requiring external services.

    Example::

        backend = MemoryBackend()
        backend.register("etl_users", row_count=5000, error_rate=0.0)
        metrics = backend.fetch("etl_users")
    """

    def __init__(self) -> None:
        self._store: dict[str, PipelineMetrics] = {}

    def register(
        self,
        pipeline_id: str,
        last_run: Optional[datetime] = None,
        last_success: Optional[datetime] = None,
        row_count: Optional[int] = None,
        error_rate: Optional[float] = None,
        duration_seconds: Optional[float] = None,
        **extra,
    ) -> None:
        """Register or update metrics for a pipeline."""
        self._store[pipeline_id] = PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=last_run,
            last_success=last_success,
            row_count=row_count,
            error_rate=error_rate,
            duration_seconds=duration_seconds,
            extra=extra,
        )

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        """Return stored metrics or raise BackendError if unknown."""
        if pipeline_id not in self._store:
            raise BackendError(
                f"Pipeline {pipeline_id!r} not found in MemoryBackend."
            )
        return self._store[pipeline_id]

    def list_pipelines(self) -> list[str]:
        """Return all registered pipeline IDs."""
        return list(self._store.keys())

    def clear(self) -> None:
        """Remove all registered pipelines."""
        self._store.clear()
