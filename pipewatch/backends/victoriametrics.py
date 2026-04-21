"""VictoriaMetrics backend for pipewatch."""
from __future__ import annotations

import urllib.request
import urllib.parse
import json
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class VictoriaMetricsBackend(BackendBase):
    """Read pipeline metrics from a VictoriaMetrics / Prometheus-compatible instance."""

    def __init__(
        self,
        base_url: str = "http://localhost:8428",
        timeout: int = 10,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _query(self, promql: str) -> list:
        """Run an instant query and return the list of result vectors."""
        url = (
            f"{self._base_url}/api/v1/query"
            f"?query={urllib.parse.quote(promql)}"
        )
        with urllib.request.urlopen(url, timeout=self._timeout) as resp:
            if resp.status != 200:
                raise RuntimeError(
                    f"VictoriaMetrics query failed: HTTP {resp.status}"
                )
            payload = json.loads(resp.read().decode())
        return payload.get("data", {}).get("result", [])

    @staticmethod
    def _parse_ts(value: Optional[str]) -> Optional[datetime]:
        if value is None:
            return None
        try:
            ts = float(value)
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # BackendBase interface
    # ------------------------------------------------------------------

    def list_pipelines(self) -> List[str]:
        results = self._query('pipeline_last_run_timestamp')
        ids = sorted(
            r["metric"].get("pipeline_id", "")
            for r in results
            if r.get("metric", {}).get("pipeline_id")
        )
        return ids

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        ts_results = self._query(
            f'pipeline_last_run_timestamp{{pipeline_id="{pipeline_id}"}}'
        )
        err_results = self._query(
            f'pipeline_error_count{{pipeline_id="{pipeline_id}"}}'
        )
        row_results = self._query(
            f'pipeline_row_count{{pipeline_id="{pipeline_id}"}}'
        )

        last_run: Optional[datetime] = None
        if ts_results:
            last_run = self._parse_ts(ts_results[0].get("value", [None, None])[1])

        error_count: Optional[int] = None
        if err_results:
            try:
                error_count = int(float(err_results[0]["value"][1]))
            except (IndexError, KeyError, ValueError):
                pass

        row_count: Optional[int] = None
        if row_results:
            try:
                row_count = int(float(row_results[0]["value"][1]))
            except (IndexError, KeyError, ValueError):
                pass

        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=last_run,
            error_count=error_count,
            row_count=row_count,
        )
