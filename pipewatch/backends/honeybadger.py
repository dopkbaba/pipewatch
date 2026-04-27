"""Honeybadger Insights backend for pipewatch."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib import request as urllib_request
from urllib.error import HTTPError

from pipewatch.backends.base import BackendBase, PipelineMetrics


class HoneybadgerBackend(BackendBase):
    """Fetch pipeline metrics from Honeybadger Insights events."""

    _BASE = "https://app.honeybadger.io/v2/insights"

    def __init__(
        self,
        api_key: str,
        project_id: str,
        event_type: str = "pipeline_health",
        timeout: int = 10,
    ) -> None:
        self._api_key = api_key
        self._project_id = project_id
        self._event_type = event_type
        self._timeout = timeout

    def _get(self, path: str) -> Any:
        url = f"{self._BASE}/{self._project_id}/{path}"
        req = urllib_request.Request(
            url,
            headers={
                "X-API-Key": self._api_key,
                "Accept": "application/json",
            },
        )
        try:
            with urllib_request.urlopen(req, timeout=self._timeout) as resp:
                if resp.status != 200:
                    raise RuntimeError(
                        f"Honeybadger returned HTTP {resp.status} for {url}"
                    )
                return json.loads(resp.read().decode())
        except HTTPError as exc:
            raise RuntimeError(
                f"Honeybadger request failed: HTTP {exc.code}"
            ) from exc

    def _parse_ts(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        dt = datetime.fromisoformat(value.rstrip("Z"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def list_pipelines(self) -> List[str]:
        data = self._get(f"events?event_type={self._event_type}")
        ids: List[str] = []
        for event in data.get("results", []):
            pid = event.get("pipeline_id")
            if pid and pid not in ids:
                ids.append(pid)
        return sorted(ids)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        data = self._get(
            f"events?event_type={self._event_type}&pipeline_id={pipeline_id}&limit=1"
        )
        results: List[Dict[str, Any]] = data.get("results", [])
        if not results:
            return PipelineMetrics(pipeline_id=pipeline_id)
        row = results[0]
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(row.get("last_run")),
            row_count=row.get("row_count"),
            error_count=row.get("error_count"),
            duration_seconds=row.get("duration_seconds"),
        )
