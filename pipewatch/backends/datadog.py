"""Datadog metrics backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

import requests

from pipewatch.backends.base import BackendBase, PipelineMetrics

_DD_API = "https://api.datadoghq.com/api/v1"


class DatadogBackend(BackendBase):
    """Fetch pipeline metrics stored as Datadog metrics/service-checks."""

    def __init__(
        self,
        api_key: str,
        app_key: str,
        metric_prefix: str = "pipewatch",
        host: str = _DD_API,
        timeout: int = 10,
    ) -> None:
        self._api_key = api_key
        self._app_key = app_key
        self._prefix = metric_prefix.rstrip(".")
        self._host = host.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {"DD-API-KEY": api_key, "DD-APPLICATION-KEY": app_key}
        )

    def _query(self, metric: str, pipeline_id: str) -> Optional[float]:
        """Return the latest value for *metric* tagged with pipeline_id."""
        import time

        now = int(time.time())
        params = {
            "from": now - 3600,
            "to": now,
            "query": f"{metric}{{pipeline_id:{pipeline_id}}}",
        }
        resp = self._session.get(
            f"{self._host}/query", params=params, timeout=self._timeout
        )
        resp.raise_for_status()
        series = resp.json().get("series", [])
        if not series:
            return None
        pointlist = series[0].get("pointlist", [])
        if not pointlist:
            return None
        return float(pointlist[-1][1])

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        last_run: Optional[datetime] = None
        row_count: Optional[int] = None
        error_count: Optional[int] = None
        duration_seconds: Optional[float] = None

        ts_val = self._query(f"{self._prefix}.last_run_ts", pipeline_id)
        if ts_val is not None:
            last_run = datetime.fromtimestamp(ts_val, tz=timezone.utc)

        rc = self._query(f"{self._prefix}.row_count", pipeline_id)
        if rc is not None:
            row_count = int(rc)

        ec = self._query(f"{self._prefix}.error_count", pipeline_id)
        if ec is not None:
            error_count = int(ec)

        dur = self._query(f"{self._prefix}.duration_seconds", pipeline_id)
        if dur is not None:
            duration_seconds = dur

        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=last_run,
            row_count=row_count,
            error_count=error_count,
            duration_seconds=duration_seconds,
        )

    def list_pipelines(self) -> List[str]:
        resp = self._session.get(
            f"{self._host}/metrics",
            params={"q": f"{self._prefix}.last_run_ts"},
            timeout=self._timeout,
        )
        resp.raise_for_status()
        metrics = resp.json().get("metrics", [])
        ids: set = set()
        for m in metrics:
            # metric names look like: pipewatch.last_run_ts{pipeline_id:foo}
            # tags come from a separate search; use metric list as signal only
            _ = m  # pipeline ids resolved via tag search not available here
        # fall back: return sorted unique ids found via active series
        series_resp = self._session.get(
            f"{self._host}/query",
            params={"query": f"{self._prefix}.last_run_ts{{*}}"},
            timeout=self._timeout,
        )
        if series_resp.ok:
            for s in series_resp.json().get("series", []):
                for tag in s.get("tags", []):
                    if tag.startswith("pipeline_id:"):
                        ids.add(tag.split(":", 1)[1])
        return sorted(ids)
