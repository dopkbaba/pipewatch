"""MQTT backend — subscribes to a topic and caches the latest metrics per pipeline."""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class MQTTBackend(BackendBase):
    """Read pipeline metrics published to an MQTT broker.

    Each message payload must be a JSON object with the keys:
      pipeline_id, last_run (ISO-8601), row_count, error_count
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        topic: str = "pipewatch/#",
        keepalive: int = 60,
    ) -> None:
        self._host = host
        self._port = port
        self._topic = topic
        self._keepalive = keepalive
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._client = self._connect()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self):
        import paho.mqtt.client as mqtt  # type: ignore

        client = mqtt.Client()
        client.on_message = self._on_message
        client.connect(self._host, self._port, self._keepalive)
        client.subscribe(self._topic)
        client.loop_start()
        return client

    def _on_message(self, _client, _userdata, message) -> None:
        try:
            payload = json.loads(message.payload.decode())
            pipeline_id = payload.get("pipeline_id")
            if pipeline_id:
                with self._lock:
                    self._cache[pipeline_id] = payload
        except (json.JSONDecodeError, AttributeError):
            pass

    @staticmethod
    def _parse_ts(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    # ------------------------------------------------------------------
    # BackendBase interface
    # ------------------------------------------------------------------

    def list_pipelines(self) -> List[str]:
        with self._lock:
            return sorted(self._cache.keys())

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        with self._lock:
            data = self._cache.get(pipeline_id, {})
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(data.get("last_run")),
            row_count=data.get("row_count"),
            error_count=data.get("error_count"),
        )
