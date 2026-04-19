"""Google Cloud Pub/Sub backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class PubSubBackend(BackendBase):
    """Read pipeline metrics published to a Pub/Sub subscription."""

    def __init__(
        self,
        project: str,
        subscription: str,
        max_messages: int = 100,
        client=None,
    ) -> None:
        self._project = project
        self._subscription = subscription
        self._max_messages = max_messages
        self._client = client or self._connect()
        self._cache: dict[str, PipelineMetrics] = {}
        self._refresh()

    def _connect(self):
        from google.cloud import pubsub_v1  # type: ignore

        return pubsub_v1.SubscriberClient()

    def _parse_ts(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _refresh(self) -> None:
        sub_path = self._client.subscription_path(self._project, self._subscription)
        response = self._client.pull(
            request={"subscription": sub_path, "max_messages": self._max_messages}
        )
        ack_ids = []
        for msg in response.received_messages:
            attrs = msg.message.attributes
            pipeline_id = attrs.get("pipeline_id")
            if not pipeline_id:
                continue
            self._cache[pipeline_id] = PipelineMetrics(
                pipeline_id=pipeline_id,
                last_run=self._parse_ts(attrs.get("last_run")),
                row_count=int(attrs["row_count"]) if attrs.get("row_count") else None,
                error_count=int(attrs["error_count"]) if attrs.get("error_count") else None,
                duration_seconds=float(attrs["duration_seconds"]) if attrs.get("duration_seconds") else None,
            )
            ack_ids.append(msg.ack_id)
        if ack_ids:
            self._client.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        return self._cache.get(pipeline_id, PipelineMetrics(pipeline_id=pipeline_id))

    def list_pipelines(self) -> List[str]:
        return sorted(self._cache.keys())
