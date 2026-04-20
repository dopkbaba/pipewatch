"""Tests for the Slack alerting backend."""
from __future__ import annotations

import json
import urllib.error
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.slack_alert import SlackAlertBackend, SlackAlertConfig


WEBHOOK_URL = "https://hooks.slack.com/services/T000/B000/xxxx"


def _make_backend(**kwargs) -> SlackAlertBackend:
    cfg = SlackAlertConfig(webhook_url=WEBHOOK_URL, **kwargs)
    return SlackAlertBackend(cfg)


def _make_event(status: str = "critical", message: str = "Pipeline is late") -> AlertEvent:
    return AlertEvent(pipeline_id="orders_etl", status=status, message=message)


class TestSlackAlertBackendPayload:
    def test_payload_contains_pipeline_id(self):
        backend = _make_backend()
        event = _make_event()
        payload = backend._build_payload(event)
        assert "orders_etl" in payload["text"]

    def test_payload_contains_status_upper(self):
        backend = _make_backend()
        event = _make_event(status="warning")
        payload = backend._build_payload(event)
        assert "WARNING" in payload["text"]

    def test_payload_sets_channel_when_provided(self):
        backend = _make_backend(channel="#alerts")
        payload = backend._build_payload(_make_event())
        assert payload["channel"] == "#alerts"

    def test_payload_omits_channel_when_not_set(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        assert "channel" not in payload

    def test_payload_uses_custom_username(self):
        backend = _make_backend(username="etl-bot")
        payload = backend._build_payload(_make_event())
        assert payload["username"] == "etl-bot"

    def test_ok_status_uses_check_mark_emoji(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="ok"))
        assert ":white_check_mark:" in payload["text"]


class TestSlackAlertBackendSend:
    def test_send_posts_json_to_webhook(self):
        backend = _make_backend()
        fake_response = MagicMock()
        fake_response.__enter__ = lambda s: s
        fake_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=fake_response) as mock_open:
            backend.send(_make_event())

        mock_open.assert_called_once()
        request = mock_open.call_args[0][0]
        assert request.full_url == WEBHOOK_URL
        body = json.loads(request.data.decode())
        assert "orders_etl" in body["text"]

    def test_send_raises_on_http_error(self):
        backend = _make_backend()
        http_err = urllib.error.HTTPError(
            WEBHOOK_URL, 403, "Forbidden", {}, BytesIO(b"")
        )
        with patch("urllib.request.urlopen", side_effect=http_err):
            with pytest.raises(RuntimeError, match="HTTP 403"):
                backend.send(_make_event())

    def test_send_raises_on_url_error(self):
        backend = _make_backend()
        url_err = urllib.error.URLError(reason="Name or service not known")
        with patch("urllib.request.urlopen", side_effect=url_err):
            with pytest.raises(RuntimeError, match="request failed"):
                backend.send(_make_event())
