"""HTTP backend for fetching pipeline metrics from a remote JSON endpoint."""

from __future__ import annotations

import datetime
from typing import List, Optional

try:
    import requests
except ImportError as exc:  # pragma: no cover
    raise ImportError("Install 'requests' to use the HTTP backend: pip install requests") from exc

from pipewatch.backends.base import BackendBase, PipelineMetrics


class HttpBackend(BackendBase):
    """Fetch pipeline metrics from a JSON REST endpoint.

    Expected response shape for ``GET <base_url>/pipelines``:
        [{"id": "...", "last_run": "<iso8601>", "last_duration_seconds": 42,
          "last_record_count": 1000, "last_error": null}, ...]

    Expected response shape for ``GET <base_url>/pipelines/<pipeline_id>``:
        {"id": "...", "last_run": "<iso8601>", ...}
    """

    name = "http"

    def __init__(self, base_url: str, timeout: int = 10, headers: Optional[dict] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.headers = headers or {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_metrics(self, data: dict) -> PipelineMetrics:
        last_run: Optional[datetime.datetime] = None
        if data.get("last_run"):
            last_run = datetime.datetime.fromisoformat(data["last_run"])
            if last_run.tzinfo is None:
                last_run = last_run.replace(tzinfo=datetime.timezone.utc)
        return PipelineMetrics(
            pipeline_id=data["id"],
            last_run=last_run,
            last_duration_seconds=data.get("last_duration_seconds"),
            last_record_count=data.get("last_record_count"),
            last_error=data.get("last_error"),
        )

    def _get(self, url: str) -> requests.Response:
        """Perform a GET request and raise a descriptive error on failure."""
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.Timeout as exc:
            raise TimeoutError(f"Request to {url!r} timed out after {self.timeout}s") from exc
        except requests.exceptions.HTTPError as exc:
            raise RuntimeError(
                f"HTTP {response.status_code} error fetching {url!r}: {response.text[:200]}"
            ) from exc
        return response

    # ------------------------------------------------------------------
    # BackendBase interface
    # ------------------------------------------------------------------

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        url = f"{self.base_url}/pipelines/{pipeline_id}"
        return self._parse_metrics(self._get(url).json())

    def list_pipelines(self) -> List[str]:
        url = f"{self.base_url}/pipelines"
        return [item["id"] for item in self._get(url).json()]
