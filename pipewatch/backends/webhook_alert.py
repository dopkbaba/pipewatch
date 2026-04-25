"""Generic webhook alert backend for pipewatch."""
from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from pipewatch.alerting import AlertEvent


@dataclass
class WebhookAlertConfig:
    url: str
    method: str = "POST"
    headers: Dict[str, str] = field(default_factory=dict)
    timeout: int = 10
    # Optional static extra fields merged into every payload
    extra: Dict[str, Any] = field(default_factory=dict)


class WebhookAlertBackend:
    """Send alert events to an arbitrary HTTP webhook endpoint."""

    def __init__(self, config: WebhookAlertConfig) -> None:
        self._cfg = config

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_payload(self, event: AlertEvent) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "pipeline_id": event.pipeline_id,
            "status": event.status.value.upper(),
            "message": event.message,
        }
        if event.metric_value is not None:
            payload["metric_value"] = event.metric_value
        payload.update(self._cfg.extra)
        return payload

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        headers = {"Content-Type": "application/json", **self._cfg.headers}
        req = urllib.request.Request(
            self._cfg.url,
            data=data,
            headers=headers,
            method=self._cfg.method,
        )
        with urllib.request.urlopen(req, timeout=self._cfg.timeout) as resp:
            if resp.status >= 400:
                raise RuntimeError(
                    f"Webhook alert failed: HTTP {resp.status} from {self._cfg.url}"
                )
