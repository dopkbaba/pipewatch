"""Prometheus/Pushgateway backend for pipewatch."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import List, Optional

import requests

from pipewatch.backends.base import BackendBase, PipelineMetrics


class PrometheusBackend(BackendBase):
    """Fetch pipeline metrics exposed via a Prometheus-compatible HTTP endpoint.

    Expects metrics in the form::

        pipeline_last_run_timestamp{pipeline="<id>"} <unix_ts>
        pipeline_row_count{pipeline="<id>"} <count>
        pipeline_error_count{pipeline="<id>"} <count>
    """

    name = "prometheus"

    def __init__(self, base_url: str, timeout: int = 10) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._metrics_url = f"{self.base_url}/metrics"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_raw(self) -> str:
        resp = requests.get(self._metrics_url, timeout=self.timeout)
        if resp.status_code != 200:
            raise ConnectionError(
                f"Prometheus endpoint returned {resp.status_code}: {self._metrics_url}"
            )
        return resp.text

    @staticmethod
    def _parse_raw(raw: str) -> dict:
        """Return {pipeline_id: {metric_name: value}} mapping."""
        pattern = re.compile(
            r'^(pipeline_\w+)\{pipeline="([^"]+)"\}\s+([\d.]+)',
            re.MULTILINE,
        )
        data: dict = {}
        for match in pattern.finditer(raw):
            metric, pid, value = match.group(1), match.group(2), float(match.group(3))
            data.setdefault(pid, {})[metric] = value
        return data

    # ------------------------------------------------------------------
    # BackendBase interface
    # ------------------------------------------------------------------

    def list_pipelines(self) -> List[str]:
        raw = self._fetch_raw()
        return sorted(self._parse_raw(raw).keys())

    def fetch(self, pipeline_id: str) -> Optional[PipelineMetrics]:
        raw = self._fetch_raw()
        parsed = self._parse_raw(raw)
        if pipeline_id not in parsed:
            return None
        entry = parsed[pipeline_id]
        last_run: Optional[datetime] = None
        ts = entry.get("pipeline_last_run_timestamp")
        if ts is not None:
            last_run = datetime.fromtimestamp(ts, tz=timezone.utc)
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=last_run,
            row_count=int(entry["pipeline_row_count"]) if "pipeline_row_count" in entry else None,
            error_count=int(entry["pipeline_error_count"]) if "pipeline_error_count" in entry else None,
        )
