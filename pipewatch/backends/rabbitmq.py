"""RabbitMQ backend — reads pipeline metrics from a queue."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class RabbitMQBackend(BackendBase):
    """Consume pipeline metric messages from a RabbitMQ queue."""

    def __init__(self, host: str = "localhost", port: int = 5672,
                 queue: str = "pipewatch", username: str = "guest",
                 password: str = "guest", max_messages: int = 200):
        self._host = host
        self._port = port
        self._queue = queue
        self._username = username
        self._password = password
        self._max_messages = max_messages
        self._store: dict[str, PipelineMetrics] = {}
        self._connected = False
        self._channel = None
        self._connection = None

    def _connect(self):
        import pika  # type: ignore
        credentials = pika.PlainCredentials(self._username, self._password)
        params = pika.ConnectionParameters(
            host=self._host, port=self._port, credentials=credentials
        )
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self._queue, durable=True)
        self._connected = True

    def _parse_ts(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _refresh(self):
        if not self._connected:
            self._connect()
        for _ in range(self._max_messages):
            method, _props, body = self._channel.basic_get(self._queue, auto_ack=True)
            if method is None:
                break
            try:
                data = json.loads(body)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            pid = data.get("pipeline_id")
            if not pid:
                continue
            self._store[pid] = PipelineMetrics(
                pipeline_id=pid,
                last_run=self._parse_ts(data.get("last_run")),
                row_count=data.get("row_count"),
                error_rate=data.get("error_rate"),
            )

    def list_pipelines(self) -> List[str]:
        self._refresh()
        return sorted(self._store.keys())

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        self._refresh()
        return self._store.get(pipeline_id, PipelineMetrics(pipeline_id=pipeline_id))
