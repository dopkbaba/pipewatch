"""Tests for the Discord alert backend."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.discord import DiscordAlertBackend, DiscordAlertConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_backend(
    webhook_url: str = "https://discord.com/api/webhooks/test/token",
    username: str = "pipewatch",
    avatar_url: str | None = None,
    mention_role_id: str | None = None,
) -> DiscordAlertBackend:
    cfg = DiscordAlertConfig(
        webhook_url=webhook_url,
        username=username,
        avatar_url=avatar_url,
        mention_role_id=mention_role_id,
    )
    return DiscordAlertBackend(cfg)


def _make_event(status: str = "CRITICAL", message: str = "Pipeline stale") -> AlertEvent:
    return AlertEvent(pipeline_id="etl.daily", status=status, message=message)


def _mock_urlopen(status: int = 204):
    """Return a context-manager mock whose .status equals *status*."""
    resp = MagicMock()
    resp.status = status
    resp.__enter__ = lambda s: resp
    resp.__exit__ = MagicMock(return_value=False)
    return patch("pipewatch.backends.discord.urllib.request.urlopen", return_value=resp)


# ---------------------------------------------------------------------------
# Payload tests
# ---------------------------------------------------------------------------

class TestDiscordPayload:
    def test_embed_title_contains_pipeline_id(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        assert "etl.daily" in payload["embeds"][0]["title"]

    def test_embed_title_contains_status_upper(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="warning"))
        assert "WARNING" in payload["embeds"][0]["title"]

    def test_critical_uses_red_colour(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="CRITICAL"))
        assert payload["embeds"][0]["color"] == 15158332

    def test_ok_uses_green_colour(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="OK"))
        assert payload["embeds"][0]["color"] == 3066993

    def test_username_in_payload(self):
        backend = _make_backend(username="alertbot")
        payload = backend._build_payload(_make_event())
        assert payload["username"] == "alertbot"

    def test_avatar_url_included_when_set(self):
        backend = _make_backend(avatar_url="https://example.com/avatar.png")
        payload = backend._build_payload(_make_event())
        assert payload["avatar_url"] == "https://example.com/avatar.png"

    def test_avatar_url_absent_when_not_set(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        assert "avatar_url" not in payload

    def test_mention_role_added_for_critical(self):
        backend = _make_backend(mention_role_id="123456789")
        payload = backend._build_payload(_make_event(status="CRITICAL"))
        assert "<@&123456789>" in payload.get("content", "")

    def test_mention_role_absent_for_ok(self):
        backend = _make_backend(mention_role_id="123456789")
        payload = backend._build_payload(_make_event(status="OK"))
        assert "content" not in payload

    def test_custom_message_in_description(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(message="Row count dropped"))
        assert "Row count dropped" in payload["embeds"][0]["description"]


# ---------------------------------------------------------------------------
# send() integration tests
# ---------------------------------------------------------------------------

class TestDiscordSend:
    def test_send_posts_json_to_webhook(self):
        backend = _make_backend()
        with _mock_urlopen(204) as mock_urlopen:
            backend.send(_make_event())
        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data)
        assert body["embeds"][0]["title"] is not None

    def test_send_raises_on_unexpected_status(self):
        backend = _make_backend()
        with _mock_urlopen(500):
            with pytest.raises(RuntimeError, match="500"):
                backend.send(_make_event())
