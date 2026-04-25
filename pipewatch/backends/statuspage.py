"""Statuspage.io alert backend for pipewatch."""
from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent


@dataclass
class StatuspageAlertConfig:
    api_key: str
    page_id: str
    component_id: str
    base_url: str = "https://api.statuspage.io/v1"
    timeout: int = 10


_STATUS_MAP = {
    "ok": "operational",
    "warning": "degraded_performance",
    "critical": "major_outage",
    "unknown": "under_maintenance",
}


class StatuspageAlertBackend:
    """Sends pipeline health alerts to a Statuspage.io component."""

    def __init__(self, config: StatuspageAlertConfig) -> None:
        self._cfg = config

    def _build_payload(self, event: AlertEvent) -> dict:
        status = _STATUS_MAP.get(event.status.lower(), "under_maintenance")
        return {
            "component": {
                "status": status,
                "description": (
                    f"[pipewatch] {event.pipeline_id}: {event.message}"
                ),
            }
        }

    def send(self, event: AlertEvent) -> None:
        cfg = self._cfg
        url = (
            f"{cfg.base_url}/pages/{cfg.page_id}"
            f"/components/{cfg.component_id}"
        )
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            url,
            data=data,
            method="PATCH",
            headers={
                "Authorization": f"OAuth {cfg.api_key}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=cfg.timeout) as resp:
            if resp.status not in (200, 201):
                raise RuntimeError(
                    f"Statuspage API error {resp.status}: {resp.read()}"
                )
