"""Webhook backend — fetches pipeline metrics from an HTTP webhook endpoint.

Expected JSON response shape (per pipeline)::

    {
        "pipeline_id": "my_pipeline",
        "last_run": "2024-01-15T10:30:00Z",  # ISO-8601, optional
        "error_rate": 0.02,                   # optional
        "row_count": 15000                    # optional
    }

The ``list`` endpoint should return a JSON array of such objects.
"""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

import requests

from pipewatch.backends.base import BackendBase, PipelineMetrics


class WebhookBackend(BackendBase):
    """Pull metrics from a user-supplied webhook URL."""

    def __init__(self, base_url: str, timeout: int = 10, token: Optional[str] = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._headers: Dict[str, str] = {}
        if token:
            self._headers["Authorization"] = f"Bearer {token}"

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _get(self, path: str) -> Any:
        url = f"{self._base_url}{path}"
        resp = requests.get(url, headers=self._headers, timeout=self._timeout)
        if resp.status_code != 200:
            raise RuntimeError(f"Webhook backend returned HTTP {resp.status_code} for {url}")
        return resp.json()

    @staticmethod
    def _parse_ts(value: Optional[str]) -> Optional[datetime.datetime]:
        if not value:
            return None
        dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt

    @staticmethod
    def _to_metrics(data: Dict[str, Any]) -> PipelineMetrics:
        return PipelineMetrics(
            pipeline_id=data.get("pipeline_id", ""),
            last_run=WebhookBackend._parse_ts(data.get("last_run")),
            error_rate=data.get("error_rate"),
            row_count=data.get("row_count"),
        )

    # ------------------------------------------------------------------
    # BackendBase interface
    # ------------------------------------------------------------------

    def list_pipelines(self) -> List[str]:
        items: List[Dict[str, Any]] = self._get("/pipelines")
        return sorted(item["pipeline_id"] for item in items if "pipeline_id" in item)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        data: Dict[str, Any] = self._get(f"/pipelines/{pipeline_id}")
        return self._to_metrics(data)
