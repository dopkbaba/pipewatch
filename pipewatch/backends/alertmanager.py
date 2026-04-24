"""Prometheus Alertmanager alert backend for pipewatch."""
from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent


@dataclass
class AlertmanagerConfig:
    url: str = "http://localhost:9093"
    generator_url: str = "http://pipewatch/"
    labels: dict = field(default_factory=dict)
    timeout: int = 10


class AlertmanagerAlertBackend:
    """Send firing alerts to a Prometheus Alertmanager instance."""

    def __init__(self, config: Optional[AlertmanagerConfig] = None) -> None:
        self._cfg = config or AlertmanagerConfig()

    def _build_payload(self, event: AlertEvent) -> list[dict]:
        labels = {
            "alertname": "PipewatchPipelineAlert",
            "pipeline": event.pipeline_id,
            "severity": event.status.value.lower(),
            **self._cfg.labels,
        }
        annotations = {
            "summary": str(event),
            "status": event.status.value,
        }
        if event.message:
            annotations["description"] = event.message
        return [
            {
                "labels": labels,
                "annotations": annotations,
                "generatorURL": self._cfg.generator_url,
            }
        ]

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        url = self._cfg.url.rstrip("/") + "/api/v2/alerts"
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self._cfg.timeout) as resp:
            if resp.status not in (200, 201, 204):
                raise RuntimeError(
                    f"Alertmanager returned HTTP {resp.status} for pipeline "
                    f"'{event.pipeline_id}'"
                )
