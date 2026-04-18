"""Splunk backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

import requests

from pipewatch.backends.base import BackendBase, PipelineMetrics


class SplunkBackend(BackendBase):
    """Fetch pipeline metrics from Splunk via the REST search API."""

    def __init__(
        self,
        base_url: str = "http://localhost:8089",
        token: str = "",
        index: str = "pipewatch",
        verify_ssl: bool = True,
        timeout: int = 10,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._index = index
        self._verify_ssl = verify_ssl
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {self._token}"

    def _search(self, spl: str) -> dict:
        resp = self._session.post(
            f"{self._base_url}/services/search/jobs/export",
            data={"search": f"search {spl}", "output_mode": "json"},
            verify=self._verify_ssl,
            timeout=self._timeout,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Splunk search failed: {resp.status_code}")
        results = []
        for line in resp.text.splitlines():
            import json
            try:
                obj = json.loads(line)
                if obj.get("result"):
                    results.append(obj["result"])
            except Exception:
                pass
        return results

    def _parse_ts(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return None

    def list_pipelines(self) -> List[str]:
        rows = self._search(
            f"index={self._index} | stats count by pipeline_id | fields pipeline_id"
        )
        return sorted(r["pipeline_id"] for r in rows if "pipeline_id" in r)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        rows = self._search(
            f"index={self._index} pipeline_id={pipeline_id} "
            "| stats latest(last_run) as last_run, latest(row_count) as row_count, "
            "latest(error_rate) as error_rate by pipeline_id"
        )
        if not rows:
            return PipelineMetrics(pipeline_id=pipeline_id)
        r = rows[0]
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(r.get("last_run")),
            row_count=int(r["row_count"]) if r.get("row_count") else None,
            error_rate=float(r["error_rate"]) if r.get("error_rate") else None,
        )
