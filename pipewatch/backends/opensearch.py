"""OpenSearch backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class OpenSearchBackend(BackendBase):
    """Fetch pipeline metrics from an OpenSearch index."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        index: str = "pipewatch",
        scheme: str = "http",
        http_auth: Optional[tuple] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._index = index
        self._scheme = scheme
        self._http_auth = http_auth
        self._client = self._connect()

    def _connect(self) -> Any:
        from opensearchpy import OpenSearch  # type: ignore

        return OpenSearch(
            hosts=[{"host": self._host, "port": self._port}],
            http_auth=self._http_auth,
            use_ssl=self._scheme == "https",
        )

    @staticmethod
    def _parse_ts(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        try:
            resp = self._client.get(index=self._index, id=pipeline_id)
        except Exception:
            return PipelineMetrics(pipeline_id=pipeline_id)
        src: Dict[str, Any] = resp.get("_source", {})
        if not src:
            return PipelineMetrics(pipeline_id=pipeline_id)
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(src.get("last_run")),
            row_count=src.get("row_count"),
            error_count=src.get("error_count"),
        )

    def list_pipelines(self) -> List[str]:
        resp = self._client.search(
            index=self._index,
            body={"query": {"match_all": {}}, "_source": False, "size": 1000},
        )
        hits = resp.get("hits", {}).get("hits", [])
        return sorted(h["_id"] for h in hits)
