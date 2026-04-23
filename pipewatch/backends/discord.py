"""Discord webhook alert backend for pipewatch."""
from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent


@dataclass
class DiscordAlertConfig:
    webhook_url: str
    username: str = "pipewatch"
    avatar_url: Optional[str] = None
    mention_role_id: Optional[str] = None


# Colour codes used by Discord embeds (decimal)
_STATUS_COLOUR = {
    "CRITICAL": 15158332,  # red
    "WARNING": 16776960,   # yellow
    "OK": 3066993,         # green
    "UNKNOWN": 9807270,    # grey
}


class DiscordAlertBackend:
    """Send pipeline health alerts to a Discord channel via an incoming webhook."""

    def __init__(self, config: DiscordAlertConfig) -> None:
        self._cfg = config

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_payload(self, event: AlertEvent) -> dict:
        status = event.status.upper()
        colour = _STATUS_COLOUR.get(status, _STATUS_COLOUR["UNKNOWN"])

        embed = {
            "title": f"[{status}] Pipeline: {event.pipeline_id}",
            "description": event.message or f"Pipeline health is {status}.",
            "color": colour,
            "fields": [
                {"name": "Pipeline", "value": event.pipeline_id, "inline": True},
                {"name": "Status", "value": status, "inline": True},
            ],
        }

        content = None
        if self._cfg.mention_role_id and status in ("CRITICAL", "WARNING"):
            content = f"<@&{self._cfg.mention_role_id}>"

        payload: dict = {
            "username": self._cfg.username,
            "embeds": [embed],
        }
        if content:
            payload["content"] = content
        if self._cfg.avatar_url:
            payload["avatar_url"] = self._cfg.avatar_url

        return payload

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send(self, event: AlertEvent) -> None:
        """POST the alert payload to the configured Discord webhook URL."""
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            self._cfg.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            if resp.status not in (200, 204):
                raise RuntimeError(
                    f"Discord webhook returned unexpected status {resp.status}"
                )
