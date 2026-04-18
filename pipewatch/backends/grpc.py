"""gRPC backend — fetches pipeline metrics from a gRPC service."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class GrpcBackend(BackendBase):
    """Fetch metrics via a gRPC endpoint.

    The remote service must expose two RPCs that map to the stubs below.
    In production the caller injects a real stub; in tests a fake is used.
    """

    def __init__(self, stub, timeout: float = 5.0) -> None:
        self._stub = stub
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_ts(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    # ------------------------------------------------------------------
    # BackendBase interface
    # ------------------------------------------------------------------

    def list_pipelines(self) -> List[str]:
        response = self._stub.ListPipelines({}, timeout=self._timeout)
        return sorted(response.pipeline_ids)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        response = self._stub.GetMetrics(
            {"pipeline_id": pipeline_id}, timeout=self._timeout
        )
        if not response or not response.pipeline_id:
            return PipelineMetrics(pipeline_id=pipeline_id)
        return PipelineMetrics(
            pipeline_id=response.pipeline_id,
            last_run=self._parse_ts(response.last_run),
            row_count=response.row_count if response.row_count else None,
            error_count=response.error_count if response.error_count else None,
            duration_seconds=(
                response.duration_seconds if response.duration_seconds else None
            ),
        )
