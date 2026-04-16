"""Elasticsearch backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from elasticsearch import Elasticsearch
except ImportError as exc:  # pragma: no cover
    raise ImportError("elasticsearch-py is required: pip install elasticsearch") from exc

from pipewatch.backends.base import BackendBase, PipelineMetrics


class ElasticsearchBackend(BackendBase):
    """Fetch pipeline metrics stored as documents in an Elasticsearch index."""

    def __init__(
        self,
        hosts: List[str] | None = None,
        index: str = "pipewatch",
        **kwargs: Any,
    ) -> None:
        self._hosts = hosts or ["http://localhost:9200"]
        self._index = index
        self._kwargs = kwargs
        self._client: Optional[Elasticsearch] = None

    def _connect(self) -> Elasticsearch:
        if self._client is None:
            self._client = Elasticsearch(self._hosts, **self._kwargs)
        return self._client

    @staticmethod
    def _parse_ts(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        dt = datetime.fromisoformat(str(value))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        client = self._connect()
        try:
            resp = client.get(index=self._index, id=pipeline_id)
            src: Dict[str, Any] = resp["_source"]
        except Exception:
            return PipelineMetrics(pipeline_id=pipeline_id)
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(src.get("last_run")),
            error_count=src.get("error_count"),
            row_count=src.get("row_count"),
            duration_seconds=src.get("duration_seconds"),
        )

    def list_pipelines(self) -> List[str]:
        client = self._connect()
        resp = client.search(
            index=self._index,
            body={"query": {"match_all": {}}, "_source": False, "size": 1000},
        )
        ids = [hit["_id"] for hit in resp["hits"]["hits"]]
        return sorted(ids)
