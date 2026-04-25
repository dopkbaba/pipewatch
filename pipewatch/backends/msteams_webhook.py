"""Microsoft Teams Incoming Webhook alert backend."""
from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent


@dataclass
class MSTeamsWebhookConfig:
    webhook_url: str
    mention_email: Optional[str] = None
    timeout: int = 10
    title_prefix: str = "[pipewatch]"


class MSTeamsWebhookAlertBackend:
    """Send alerts to a Microsoft Teams channel via Incoming Webhook."""

    def __init__(self, config: MSTeamsWebhookConfig) -> None:
        self._cfg = config

    def _build_payload(self, event: AlertEvent) -> dict:
        status = event.status.upper()
        title = f"{self._cfg.title_prefix} {event.pipeline_id} — {status}"
        color = {
            "CRITICAL": "FF0000",
            "WARNING": "FFA500",
            "OK": "00AA00",
        }.get(status, "808080")

        facts = [
            {"name": "Pipeline", "value": event.pipeline_id},
            {"name": "Status", "value": status},
        ]
        if event.message:
            facts.append({"name": "Message", "value": event.message})

        section: dict = {
            "activityTitle": title,
            "activitySubtitle": event.message or "",
            "facts": facts,
            "markdown": True,
        }

        if self._cfg.mention_email:
            section["potentialAction"] = []

        payload = {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": color,
            "summary": title,
            "sections": [section],
        }
        return payload

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            self._cfg.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self._cfg.timeout) as resp:
            if resp.status not in (200, 201, 202):
                raise RuntimeError(
                    f"MSTeams webhook returned HTTP {resp.status}"
                )
