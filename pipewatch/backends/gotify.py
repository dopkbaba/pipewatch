"""Gotify push-notification alert backend."""
from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent


@dataclass
class GotifyAlertConfig:
    url: str  # e.g. "https://gotify.example.com"
    token: str
    priority_map: dict = field(
        default_factory=lambda: {"critical": 9, "warning": 5, "ok": 1, "unknown": 3}
    )
    timeout: int = 10


class GotifyAlertBackend:
    """Send pipeline health alerts via Gotify's REST API."""

    def __init__(self, config: GotifyAlertConfig) -> None:
        self._cfg = config

    def _build_payload(self, event: AlertEvent) -> dict:
        status = event.status.upper()
        priority = self._cfg.priority_map.get(event.status.lower(), 3)
        return {
            "title": f"[{status}] Pipeline {event.pipeline_id}",
            "message": (
                f"Pipeline: {event.pipeline_id}\n"
                f"Status:   {status}\n"
                f"Reason:   {event.reason or 'n/a'}"
            ),
            "priority": priority,
        }

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        endpoint = self._cfg.url.rstrip("/") + "/message"
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            endpoint,
            data=data,
            headers={
                "Content-Type": "application/json",
                "X-Gotify-Key": self._cfg.token,
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self._cfg.timeout) as resp:
            if resp.status not in (200, 201):
                raise RuntimeError(
                    f"Gotify returned unexpected status {resp.status}"
                )
