"""SignalR (Azure SignalR Service) backend for pipewatch."""
from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class SignalRBackend(BackendBase):
    """Read pipeline metrics pushed to an Azure SignalR REST endpoint."""

    def __init__(
        self,
        base_url: str = "http://localhost:5000",
        hub: str = "pipewatch",
        timeout: int = 10,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._hub = hub
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, path: str) -> Any:
        url = f"{self._base_url}{path}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=self._timeout) as resp:
            if resp.status != 200:
                raise RuntimeError(
                    f"SignalR REST API returned HTTP {resp.status} for {url}"
                )
            return json.loads(resp.read().decode())

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
        data: List[Dict[str, Any]] = self._get(f"/hubs/{self._hub}/pipelines")
        return sorted(item["id"] for item in data)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        data: Dict[str, Any] = self._get(
            f"/hubs/{self._hub}/pipelines/{pipeline_id}"
        )
        if not data:
            return PipelineMetrics(pipeline_id=pipeline_id)
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(data.get("last_run")),
            row_count=data.get("row_count"),
            error_count=data.get("error_count"),
            duration_seconds=data.get("duration_seconds"),
        )
