"""AWS Kinesis backend for pipewatch."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class KinesisBackend(BackendBase):
    """Read pipeline metrics published to an AWS Kinesis stream."""

    def __init__(
        self,
        stream_name: str,
        region_name: str = "us-east-1",
        shard_iterator_type: str = "LATEST",
        **boto_kwargs: Any,
    ) -> None:
        self._stream_name = stream_name
        self._region = region_name
        self._iterator_type = shard_iterator_type
        self._boto_kwargs = boto_kwargs
        self._cache: Dict[str, PipelineMetrics] = {}
        self._client: Any = None

    def _connect(self) -> Any:
        if self._client is None:
            import boto3  # type: ignore

            self._client = boto3.client(
                "kinesis", region_name=self._region, **self._boto_kwargs
            )
        return self._client

    @staticmethod
    def _parse_ts(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _refresh(self) -> None:
        client = self._connect()
        shards = client.list_shards(StreamName=self._stream_name).get("Shards", [])
        for shard in shards:
            shard_id = shard["ShardId"]
            iterator = client.get_shard_iterator(
                StreamName=self._stream_name,
                ShardId=shard_id,
                ShardIteratorType=self._iterator_type,
            )["ShardIterator"]
            records = client.get_records(ShardIterator=iterator, Limit=100)["Records"]
            for record in records:
                try:
                    data = json.loads(record["Data"])
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                pid = data.get("pipeline_id")
                if not pid:
                    continue
                self._cache[pid] = PipelineMetrics(
                    pipeline_id=pid,
                    last_run=self._parse_ts(data.get("last_run")),
                    record_count=data.get("record_count"),
                    error_count=data.get("error_count"),
                    duration_seconds=data.get("duration_seconds"),
                )

    def list_pipelines(self) -> List[str]:
        self._refresh()
        return sorted(self._cache)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        self._refresh()
        return self._cache.get(pipeline_id, PipelineMetrics(pipeline_id=pipeline_id))
