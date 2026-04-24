"""Telegram alert backend for pipewatch.

Sends pipeline health alerts to a Telegram chat via the Bot API.
"""

from __future__ import annotations

import json
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

# Emoji map for visual status indication
_STATUS_EMOJI = {
    "OK": "\u2705",       # ✅
    "WARNING": "\u26a0\ufe0f",  # ⚠️
    "CRITICAL": "\U0001f6a8",  # 🚨
    "UNKNOWN": "\u2753",   # ❓
}


@dataclass
class TelegramAlertConfig:
    """Configuration for the Telegram alert backend."""

    bot_token: str
    chat_id: str
    # Optional thread id for supergroup topics
    message_thread_id: Optional[int] = None
    parse_mode: str = "MarkdownV2"
    timeout: int = 10
    # Extra static tags appended to every message
    extra_tags: dict = field(default_factory=dict)


class TelegramAlertBackend:
    """Sends alert events to a Telegram chat via the Bot API."""

    def __init__(self, config: TelegramAlertConfig) -> None:
        self._config = config

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_payload(self, event: AlertEvent) -> dict:
        """Return the JSON-serialisable payload for sendMessage."""
        cfg = self._config
        status = event.status.upper()
        emoji = _STATUS_EMOJI.get(status, "\u2139\ufe0f")

        # MarkdownV2 requires escaping several special characters.
        def _esc(text: str) -> str:
            for ch in r"\_*[]()~`>#+-=|{}.!":
                text = text.replace(ch, f"\\{ch}")
            return text

        lines = [
            f"{emoji} *Pipeline Alert*",
            f"Pipeline: `{_esc(event.pipeline_id)}`",
            f"Status:   *{_esc(status)}*",
        ]
        if event.message:
            lines.append(f"Message:  {_esc(event.message)}`)
        for key, value in cfg.extra_tags.items():
            lines.append(f"{_esc(str(key))}: {_esc(str(value))}")

        payload: dict = {
            "chat_id": cfg.chat_id,
            "text": "\n".join(lines),
            "parse_mode": cfg.parse_mode,
        }
        if cfg.message_thread_id is not None:
            payload["message_thread_id"] = cfg.message_thread_id
        return payload

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send(self, event: AlertEvent) -> None:
        """Dispatch *event* to the configured Telegram chat.

        Raises:
            RuntimeError: if the Telegram API returns a non-2xx response.
        """
        cfg = self._config
        url = _TELEGRAM_API.format(token=cfg.bot_token)
        payload = self._build_payload(event)
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=cfg.timeout) as resp:
                if resp.status < 200 or resp.status >= 300:
                    raise RuntimeError(
                        f"Telegram API returned HTTP {resp.status}"
                    )
        except urllib.error.HTTPError as exc:
            raise RuntimeError(
                f"Telegram API error: HTTP {exc.code}"
            ) from exc
