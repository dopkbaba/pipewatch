"""Pushover alert backend for pipewatch."""
from __future__ import annotations

import json
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from typing import Dict

from pipewatch.alerting import AlertEvent

_PUSHOVER_API_URL = "https://api.pushover.net/1/messages.json"

_DEFAULT_PRIORITY_MAP: Dict[str, int] = {
    "ok": -1,
    "warning": 0,
    "critical": 1,
    "unknown": 0,
}


@dataclass
class PushoverAlertConfig:
    user_key: str
    api_token: str
    priority_map: Dict[str, int] = field(default_factory=lambda: dict(_DEFAULT_PRIORITY_MAP))
    timeout: int = 10


class PushoverAlertBackend:
    """Send pipeline alerts via Pushover notifications."""

    def __init__(self, config: PushoverAlertConfig) -> None:
        self._config = config

    def _build_payload(self, event: AlertEvent) -> Dict:
        status = event.status.lower()
        priority = self._config.priority_map.get(status, 0)
        title = f"[{event.status.upper()}] Pipeline: {event.pipeline_id}"
        message_parts = [f"Status: {event.status.upper()}"]
        if event.message:
            message_parts.append(event.message)
        return {
            "token": self._config.api_token,
            "user": self._config.user_key,
            "title": title,
            "message": "\n".join(message_parts),
            "priority": priority,
        }

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        data = urllib.parse.urlencode(payload).encode()
        req = urllib.request.Request(
            _PUSHOVER_API_URL,
            data=data,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=self._config.timeout) as resp:
            if resp.status != 200:
                body = resp.read().decode()
                raise RuntimeError(
                    f"Pushover API returned {resp.status}: {body}"
                )
