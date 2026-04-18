"""Graphite backend for pipewatch."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from pipewatch.backends.base import BackendBase, PipelineMetrics


class GraphiteBackend(BackendBase):
    """Fetch pipeline metrics from a Graphite HTTP API."""

    def __init__(
        self,
        base_url: str = "http://localhost:80",
        prefix: str = "pipewatch",
        timeout: int = 10,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._prefix = prefix
        self._timeout = timeout

    def _query(self, target: str) -> List[Dict[str, Any]]:
        url = f"{self._base_url}/render"
        params = {"target": target, "format": "json", "from": "-1h"}
        resp = requests.get(url, params=params, timeout=self._timeout)
        if resp.status_code != 200:
            raise RuntimeError(f"Graphite returned {resp.status_code} for {target}")
        return resp.json()

    def list_pipelines(self) -> List[str]:
        data = self._query(f"{self._prefix}.*.last_run")
        ids = []
        for series in data:
            parts = series.get("target", "").split(".")
            if len(parts) >= 2:
                ids.append(parts[-2])
        return sorted(ids)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        last_run: Optional[datetime] = None
        error_count: Optional[int] = None
        row_count: Optional[int] = None

        for metric, field in [
            ("last_run", "last_run"),
            ("error_count", "error_count"),
            ("row_count", "row_count"),
        ]:
            target = f"{self._prefix}.{pipeline_id}.{metric}"
            try:
                data = self._query(target)
            except RuntimeError:
                continue
            if not data:
                continue
            points = [p for p in data[0].get("datapoints", []) if p[0] is not None]
            if not points:
                continue
            value = points[-1][0]
            if field == "last_run":
                last_run = datetime.fromtimestamp(float(value), tz=timezone.utc)
            elif field == "error_count":
                error_count = int(value)
            elif field == "row_count":
                row_count = int(value)

        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=last_run,
            error_count=error_count,
            row_count=row_count,
        )
