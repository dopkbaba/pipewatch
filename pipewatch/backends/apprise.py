"""Apprise multi-channel alert backend for pipewatch."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError
import json

from pipewatch.alerting import AlertEvent
from pipewatch.health import HealthStatus


@dataclass
class AppriseAlertConfig:
    """Configuration for the Apprise alert backend."""

    # Apprise stateless API endpoint, e.g. http://localhost:8000/notify
    api_url: str = "http://localhost:8000/notify"
    # Comma-separated list of Apprise notification URLs to target
    urls: str = ""
    timeout: int = 10
    # Map HealthStatus -> Apprise tag (used as notification type hint in title)
    status_emoji: Dict[str, str] = field(default_factory=lambda: {
        HealthStatus.OK: "\u2705",
        HealthStatus.WARNING: "\u26a0\ufe0f",
        HealthStatus.CRITICAL: "\U0001f6a8",
        HealthStatus.UNKNOWN: "\u2753",
    })


class AppriseAlertBackend:
    """Send alerts via the Apprise stateless HTTP API."""

    def __init__(self, config: Optional[AppriseAlertConfig] = None) -> None:
        self._cfg = config or AppriseAlertConfig()

    def _build_payload(self, event: AlertEvent) -> Dict[str, Any]:
        emoji = self._cfg.status_emoji.get(event.status, "\u2753")
        title = f"{emoji} PipeWatch [{event.status.upper()}] {event.pipeline_id}"
        body_lines = [f"Pipeline : {event.pipeline_id}", f"Status   : {event.status.upper()}"]
        if event.message:
            body_lines.append(f"Detail   : {event.message}")
        payload: Dict[str, Any] = {
            "title": title,
            "body": "\n".join(body_lines),
        }
        if self._cfg.urls:
            payload["urls"] = self._cfg.urls
        return payload

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        req = Request(
            self._cfg.api_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(req, timeout=self._cfg.timeout) as resp:
                if resp.status not in (200, 204):
                    raise RuntimeError(
                        f"Apprise API returned HTTP {resp.status}"
                    )
        except URLError as exc:
            raise RuntimeError(f"Apprise API request failed: {exc}") from exc
