"""AWS SQS backend – reads pipeline metrics from SQS messages."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class SQSBackend(BackendBase):
    """Poll an SQS queue for pipeline metric messages."""

    def __init__(
        self,
        queue_url: str,
        region_name: str = "us-east-1",
        max_messages: int = 10,
        boto_session: Optional[Any] = None,
    ) -> None:
        self._queue_url = queue_url
        self._region = region_name
        self._max_messages = max_messages
        self._session = boto_session
        self._cache: Dict[str, PipelineMetrics] = {}
        self._client: Optional[Any] = None

    def _connect(self) -> Any:
        if self._client is None:
            if self._session is not None:
                self._client = self._session.client("sqs", region_name=self._region)
            else:
                import boto3
                self._client = boto3.client("sqs", region_name=self._region)
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
        resp = client.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=self._max_messages,
            AttributeNames=["All"],
        )
        for msg in resp.get("Messages", []):
            try:
                body = json.loads(msg["Body"])
            except (json.JSONDecodeError, KeyError):
                continue
            pid = body.get("pipeline_id")
            if not pid:
                continue
            self._cache[pid] = PipelineMetrics(
                pipeline_id=pid,
                last_run=self._parse_ts(body.get("last_run")),
                rows_processed=body.get("rows_processed"),
                error_count=body.get("error_count"),
                duration_seconds=body.get("duration_seconds"),
            )

    def list_pipelines(self) -> List[str]:
        self._refresh()
        return sorted(self._cache.keys())

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        self._refresh()
        return self._cache.get(pipeline_id, PipelineMetrics(pipeline_id=pipeline_id))
