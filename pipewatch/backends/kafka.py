"""Kafka backend: reads pipeline metrics from a Kafka topic via a REST Proxy."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from pipewatch.backends.base import BackendBase, PipelineMetrics


class KafkaBackend(BackendBase):
    """Fetch pipeline metrics published to a Kafka topic via Confluent REST Proxy.

    Each message value must be a JSON object with keys:
        pipeline_id  (str)
        last_run     (ISO-8601 UTC string, optional)
        row_count    (int, optional)
        error_count  (int, optional)
    """

    def __init__(
        self,
        rest_proxy_url: str,
        topic: str,
        consumer_group: str = "pipewatch",
        timeout: int = 10,
    ) -> None:
        self._base = rest_proxy_url.rstrip("/")
        self._topic = topic
        self._group = consumer_group
        self._timeout = timeout
        self._cache: Dict[str, PipelineMetrics] = {}
        self._refresh()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _refresh(self) -> None:
        """Pull all available messages from the topic and update the cache."""
        url = f"{self._base}/topics/{self._topic}/partitions/0/messages"
        params = {"offset": 0, "count": 1000}
        headers = {"Accept": "application/vnd.kafka.json.v2+json"}
        resp = requests.get(url, params=params, headers=headers, timeout=self._timeout)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Kafka REST Proxy returned {resp.status_code} for topic '{self._topic}'"
            )
        messages: List[Dict[str, Any]] = resp.json()
        for msg in messages:
            value = msg.get("value", {})
            if not isinstance(value, dict):
                continue
            pid = value.get("pipeline_id")
            if not pid:
                continue
            self._cache[pid] = self._parse_metrics(pid, value)

    def _parse_metrics(self, pipeline_id: str, data: Dict[str, Any]) -> PipelineMetrics:
        last_run: Optional[datetime] = None
        raw_ts = data.get("last_run")
        if raw_ts:
            dt = datetime.fromisoformat(raw_ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            last_run = dt
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=last_run,
            row_count=data.get("row_count"),
            error_count=data.get("error_count"),
        )

    # ------------------------------------------------------------------
    # BackendBase interface
    # ------------------------------------------------------------------

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        return self._cache.get(
            pipeline_id,
            PipelineMetrics(pipeline_id=pipeline_id),
        )

    def list_pipelines(self) -> List[str]:
        return sorted(self._cache.keys())
