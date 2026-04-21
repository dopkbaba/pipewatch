"""VictorOps (Splunk On-Call) alert backend for pipewatch."""
from __future__ import annotations

import urllib.request
import urllib.error
import json
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent


@dataclass
class VictorOpsAlertConfig:
    """Configuration for the VictorOps alert backend."""

    routing_key: str
    rest_endpoint: str = "https://alert.victorops.com/integrations/generic/20131114/alert"
    api_key: str = ""
    timeout: int = 10


class VictorOpsAlertBackend:
    """Send alerts to VictorOps (Splunk On-Call) via the REST endpoint."""

    _MESSAGE_TYPE_MAP = {
        "critical": "CRITICAL",
        "warning": "WARNING",
        "ok": "RECOVERY",
        "unknown": "WARNING",
    }

    def __init__(self, config: VictorOpsAlertConfig) -> None:
        self._config = config

    def _build_payload(self, event: AlertEvent) -> dict:
        status = event.status.lower()
        message_type = self._MESSAGE_TYPE_MAP.get(status, "WARNING")
        return {
            "message_type": message_type,
            "entity_id": event.pipeline_id,
            "entity_display_name": f"[{event.status.upper()}] Pipeline {event.pipeline_id}",
            "state_message": event.message,
            "monitoring_tool": "pipewatch",
        }

    def send(self, event: AlertEvent) -> None:
        """Dispatch an alert event to VictorOps."""
        cfg = self._config
        url = f"{cfg.rest_endpoint.rstrip('/')}/{cfg.api_key}/{cfg.routing_key}"
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=cfg.timeout) as resp:
                if resp.status not in (200, 201):
                    raise RuntimeError(
                        f"VictorOps returned unexpected status {resp.status}"
                    )
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"VictorOps request failed: {exc}") from exc
