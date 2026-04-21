"""Microsoft Teams alert backend for pipewatch."""
from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent


@dataclass
class TeamsAlertConfig:
    webhook_url: str
    mention_on_critical: Optional[str] = None  # e.g. "<at>oncall</at>"
    timeout: int = 10
    extra_headers: dict = field(default_factory=dict)


class TeamsAlertBackend:
    """Sends pipeline health alerts to a Microsoft Teams channel via
    an Incoming Webhook."""

    def __init__(self, config: TeamsAlertConfig) -> None:
        self._cfg = config

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_payload(self, event: AlertEvent) -> dict:
        status = event.status.upper()
        color = {
            "CRITICAL": "FF0000",
            "WARNING": "FFA500",
            "OK": "00AA00",
        }.get(status, "888888")

        summary = f"[{status}] Pipeline '{event.pipeline_id}'"
        body_text = event.message or summary

        if status == "CRITICAL" and self._cfg.mention_on_critical:
            body_text = f"{self._cfg.mention_on_critical} {body_text}"

        return {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "summary": summary,
            "themeColor": color,
            "sections": [
                {
                    "activityTitle": summary,
                    "activityText": body_text,
                    "facts": [
                        {"name": "Pipeline", "value": event.pipeline_id},
                        {"name": "Status", "value": status},
                    ],
                }
            ],
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            self._cfg.webhook_url,
            data=data,
            headers={"Content-Type": "application/json", **self._cfg.extra_headers},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self._cfg.timeout) as resp:
            if resp.status not in (200, 204):
                raise RuntimeError(
                    f"Teams webhook returned HTTP {resp.status}"
                )
