"""Apache Pulsar backend for pipewatch."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class PulsarBackend(BackendBase):
    """Read pipeline metrics from an Apache Pulsar topic."""

    def __init__(
        self,
        service_url: str = "pulsar://localhost:6650",
        topic: str = "pipewatch-metrics",
        subscription: str = "pipewatch-sub",
        receive_timeout_ms: int = 3000,
    ) -> None:
        self._service_url = service_url
        self._topic = topic
        self._subscription = subscription
        self._receive_timeout_ms = receive_timeout_ms
        self._store: Dict[str, PipelineMetrics] = {}
        self._connect()

    def _connect(self) -> None:
        import pulsar  # type: ignore

        client = pulsar.Client(self._service_url)
        self._consumer = client.subscribe(
            self._topic,
            subscription_name=self._subscription,
            consumer_type=pulsar.ConsumerType.Shared,
        )
        self._refresh()

    def _refresh(self) -> None:
        import pulsar  # type: ignore

        while True:
            try:
                msg = self._consumer.receive(timeout_millis=self._receive_timeout_ms)
                self._consumer.acknowledge(msg)
                data = json.loads(msg.data().decode())
                pid = data.get("pipeline_id")
                if pid:
                    self._store[pid] = self._parse_metrics(data)
            except Exception:
                break

    def _parse_metrics(self, data: Dict[str, Any]) -> PipelineMetrics:
        last_run: Optional[datetime] = None
        raw = data.get("last_run")
        if raw:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            last_run = dt
        return PipelineMetrics(
            pipeline_id=data["pipeline_id"],
            last_run=last_run,
            record_count=data.get("record_count"),
            error_count=data.get("error_count"),
        )

    def list_pipelines(self) -> List[str]:
        return sorted(self._store.keys())

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        return self._store.get(pipeline_id, PipelineMetrics(pipeline_id=pipeline_id))
