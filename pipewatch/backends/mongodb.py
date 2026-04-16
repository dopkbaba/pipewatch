"""MongoDB backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class MongoDBBackend(BackendBase):
    """Fetch pipeline metrics from a MongoDB collection."""

    def __init__(
        self,
        uri: str = "mongodb://localhost:27017",
        database: str = "pipewatch",
        collection: str = "pipeline_metrics",
    ) -> None:
        self._uri = uri
        self._database = database
        self._collection = collection
        self._client = None

    def _connect(self):
        if self._client is None:
            import pymongo  # type: ignore

            self._client = pymongo.MongoClient(self._uri)
        return self._client[self._database][self._collection]

    @staticmethod
    def _parse_ts(value) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        return None

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        col = self._connect()
        doc = col.find_one({"pipeline_id": pipeline_id})
        if doc is None:
            return PipelineMetrics(pipeline_id=pipeline_id)
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(doc.get("last_run")),
            rows_processed=doc.get("rows_processed"),
            error_count=doc.get("error_count"),
            duration_seconds=doc.get("duration_seconds"),
        )

    def list_pipelines(self) -> List[str]:
        col = self._connect()
        ids = col.distinct("pipeline_id")
        return sorted(ids)
