"""OpsGenie alert backend for pipewatch."""
from __future__ import annotations

import urllib.request
import urllib.error
import json
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent


@dataclass
class OpsGenieAlertConfig:
    api_key: str
    base_url: str = "https://api.opsgenie.com/v2/alerts"
    tags: list[str] = field(default_factory=list)
    priority: str = "P3"  # P1-P5
    responders: list[dict] = field(default_factory=list)


class OpsGenieAlertBackend:
    """Send alerts to OpsGenie via the REST API."""

    _PRIORITY_MAP = {
        "critical": "P1",
        "warning": "P3",
        "ok": "P5",
        "unknown": "P4",
    }

    def __init__(self, config: OpsGenieAlertConfig) -> None:
        self._config = config

    def _build_payload(self, event: AlertEvent) -> dict:
        status = event.status.lower()
        priority = self._PRIORITY_MAP.get(status, self._config.priority)
        payload: dict = {
            "message": f"[{event.status.upper()}] Pipeline '{event.pipeline_id}' health alert",
            "alias": f"pipewatch-{event.pipeline_id}",
            "description": event.message or f"Pipeline {event.pipeline_id} is {status}.",
            "priority": priority,
            "source": "pipewatch",
            "tags": list(self._config.tags),
        }
        if self._config.responders:
            payload["responders"] = list(self._config.responders)
        if event.details:
            payload["details"] = {k: str(v) for k, v in event.details.items()}
        return payload

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            self._config.base_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"GenieKey {self._config.api_key}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req) as resp:
                if resp.status not in (200, 201, 202):
                    raise RuntimeError(
                        f"OpsGenie returned unexpected status {resp.status}"
                    )
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"OpsGenie request failed: {exc}") from exc
