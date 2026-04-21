"""PagerDuty alert backend for pipewatch."""
from __future__ import annotations

import urllib.request
import urllib.error
import json
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent

_EVENTS_API = "https://events.pagerduty.com/v2/enqueue"


@dataclass
class PagerDutyAlertConfig:
    integration_key: str
    severity_map: dict[str, str] = field(
        default_factory=lambda: {
            "CRITICAL": "critical",
            "WARNING": "warning",
            "OK": "info",
            "UNKNOWN": "warning",
        }
    )
    source: str = "pipewatch"
    timeout: int = 10


class PagerDutyAlertBackend:
    """Sends alerts to PagerDuty via the Events API v2."""

    def __init__(self, config: PagerDutyAlertConfig) -> None:
        self._cfg = config

    def _build_payload(self, event: AlertEvent) -> dict:
        severity = self._cfg.severity_map.get(event.status, "warning")
        return {
            "routing_key": self._cfg.integration_key,
            "event_action": "trigger",
            "payload": {
                "summary": (
                    f"[{event.status}] Pipeline '{event.pipeline_id}': {event.message}"
                ),
                "source": self._cfg.source,
                "severity": severity,
                "custom_details": {
                    "pipeline_id": event.pipeline_id,
                    "status": event.status,
                    "message": event.message,
                },
            },
        }

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            _EVENTS_API,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._cfg.timeout) as resp:
                if resp.status not in (200, 202):
                    raise RuntimeError(
                        f"PagerDuty API returned HTTP {resp.status}"
                    )
        except urllib.error.HTTPError as exc:
            raise RuntimeError(
                f"PagerDuty API error: HTTP {exc.code}"
            ) from exc
