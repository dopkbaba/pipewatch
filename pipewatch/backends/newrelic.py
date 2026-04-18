"""New Relic backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

import requests

from pipewatch.backends.base import BackendBase, PipelineMetrics


class NewRelicBackend(BackendBase):
    """Fetch pipeline metrics from New Relic Insights / NRQL."""

    _BASE = "https://insights-api.newrelic.com/v1/accounts/{account_id}/query"

    def __init__(
        self,
        account_id: str,
        api_key: str,
        table: str = "PipelineMetrics",
        timeout: int = 10,
    ) -> None:
        self._account_id = account_id
        self._api_key = api_key
        self._table = table
        self._timeout = timeout
        self._url = self._BASE.format(account_id=account_id)

    def _query(self, nrql: str) -> dict:
        resp = requests.get(
            self._url,
            headers={"X-Query-Key": self._api_key, "Accept": "application/json"},
            params={"nrql": nrql},
            timeout=self._timeout,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"New Relic query failed: {resp.status_code} {resp.text}")
        return resp.json()

    def list_pipelines(self) -> List[str]:
        nrql = f"SELECT uniques(pipeline_id) FROM {self._table}"
        data = self._query(nrql)
        members = (
            data.get("results", [{}])[0].get("members", [])
        )
        return sorted(members)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        nrql = (
            f"SELECT latest(last_run), latest(row_count), latest(error_rate) "
            f"FROM {self._table} WHERE pipeline_id = '{pipeline_id}'"
        )
        data = self._query(nrql)
        results = data.get("results", [])
        if not results:
            return PipelineMetrics(pipeline_id=pipeline_id)

        r = results[0]
        last_run: Optional[datetime] = None
        raw_ts = r.get("latest.last_run")
        if raw_ts:
            last_run = datetime.fromtimestamp(raw_ts / 1000, tz=timezone.utc)

        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=last_run,
            row_count=r.get("latest.row_count"),
            error_rate=r.get("latest.error_rate"),
        )
