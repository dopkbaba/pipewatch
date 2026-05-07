"""ntfy.sh alert backend for pipewatch."""
from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from typing import Dict

from pipewatch.alerting import AlertEvent

_DEFAULT_PRIORITY: Dict[str, int] = {
    "critical": 5,
    "warning": 3,
    "ok": 1,
    "unknown": 2,
}


@dataclass
class NtfyAlertConfig:
    server: str = "https://ntfy.sh"
    topic: str = "pipewatch"
    timeout: int = 10
    priority_map: Dict[str, int] = field(default_factory=lambda: dict(_DEFAULT_PRIORITY))


class NtfyAlertBackend:
    """Send alerts to a ntfy.sh topic."""

    def __init__(self, config: NtfyAlertConfig | None = None) -> None:
        self._cfg = config or NtfyAlertConfig()

    def _build_payload(self, event: AlertEvent) -> dict:
        status = event.status.lower()
        priority = self._cfg.priority_map.get(status, 3)
        return {
            "topic": self._cfg.topic,
            "title": f"[{event.status.upper()}] {event.pipeline_id}",
            "message": event.message or f"Pipeline {event.pipeline_id} is {status}.",
            "priority": priority,
            "tags": ["pipewatch", status],
        }

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        url = self._cfg.server.rstrip("/")
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self._cfg.timeout) as resp:  # noqa: S310
            if resp.status >= 400:
                raise RuntimeError(
                    f"ntfy returned HTTP {resp.status} for topic '{self._cfg.topic}'"
                )
