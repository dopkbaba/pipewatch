"""Slack alerting backend for PipeWatch.

Sends alert notifications to a Slack channel via the Incoming Webhooks API.
"""
from __future__ import annotations

import json
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent


@dataclass
class SlackAlertConfig:
    webhook_url: str
    channel: Optional[str] = None
    username: str = "pipewatch"
    icon_emoji: str = ":rotating_light:"
    timeout: int = 10


_STATUS_EMOJI = {
    "ok": ":white_check_mark:",
    "warning": ":warning:",
    "critical": ":red_circle:",
    "unknown": ":grey_question:",
}


class SlackAlertBackend:
    """Dispatches AlertEvents to a Slack Incoming Webhook."""

    def __init__(self, config: SlackAlertConfig) -> None:
        self._config = config

    def _build_payload(self, event: AlertEvent) -> dict:
        emoji = _STATUS_EMOJI.get(event.status.lower(), ":bell:")
        text = (
            f"{emoji} *[{event.status.upper()}]* Pipeline `{event.pipeline_id}`\n"
            f"> {event.message}"
        )
        payload: dict = {
            "username": self._config.username,
            "icon_emoji": self._config.icon_emoji,
            "text": text,
        }
        if self._config.channel:
            payload["channel"] = self._config.channel
        return payload

    def send(self, event: AlertEvent) -> None:
        """Send *event* to Slack.  Raises ``RuntimeError`` on HTTP errors."""
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            self._config.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._config.timeout):
                pass
        except urllib.error.HTTPError as exc:
            raise RuntimeError(
                f"Slack webhook returned HTTP {exc.code}: {exc.reason}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Slack webhook request failed: {exc.reason}") from exc
