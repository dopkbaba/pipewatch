"""Zenduty alert backend for pipewatch."""
from __future__ import annotations

import urllib.request
import urllib.error
import json
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent


@dataclass
class ZendutyAlertConfig:
    api_key: str
    service_id: str
    base_url: str = "https://www.zenduty.com/api/v1"
    default_urgency: int = 1  # 1=high, 2=low
    timeout: int = 10


class ZendutyAlertBackend:
    """Send alerts to Zenduty via their incidents API."""

    def __init__(self, config: ZendutyAlertConfig) -> None:
        self._cfg = config

    def _build_payload(self, event: AlertEvent) -> dict:
        urgency = 1 if event.status.upper() == "CRITICAL" else 2
        return {
            "title": f"[{event.status.upper()}] Pipeline {event.pipeline_id}",
            "message": event.message or f"Pipeline {event.pipeline_id} is {event.status}",
            "urgency": urgency,
            "service": self._cfg.service_id,
        }

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        url = f"{self._cfg.base_url}/incidents/"
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Authorization": f"Token {self._cfg.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._cfg.timeout) as resp:
                if resp.status not in (200, 201):
                    raise RuntimeError(
                        f"Zenduty returned unexpected status {resp.status}"
                    )
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"Zenduty request failed: {exc.code} {exc.reason}") from exc
