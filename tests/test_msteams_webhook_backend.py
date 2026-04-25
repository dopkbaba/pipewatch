"""Tests for the MSTeams Incoming Webhook alert backend."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.msteams_webhook import (
    MSTeamsWebhookAlertBackend,
    MSTeamsWebhookConfig,
)


def _make_backend(webhook_url="https://example.webhook.office.com/xxx", **kw):
    cfg = MSTeamsWebhookConfig(webhook_url=webhook_url, **kw)
    return MSTeamsWebhookAlertBackend(cfg)


def _make_event(pipeline_id="pipe-1", status="critical", message="stale"):
    return AlertEvent(pipeline_id=pipeline_id, status=status, message=message)


def _mock_urlopen(status=200):
    cm = MagicMock()
    cm.__enter__ = lambda s: s
    cm.__exit__ = MagicMock(return_value=False)
    cm.status = status
    return cm


class TestMSTeamsWebhookPayload:
    def test_summary_contains_pipeline_id(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        assert "pipe-1" in payload["summary"]

    def test_summary_contains_status_upper(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="warning"))
        assert "WARNING" in payload["summary"]

    def test_critical_theme_color_is_red(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="critical"))
        assert payload["themeColor"] == "FF0000"

    def test_warning_theme_color_is_orange(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="warning"))
        assert payload["themeColor"] == "FFA500"

    def test_ok_theme_color_is_green(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="ok"))
        assert payload["themeColor"] == "00AA00"

    def test_unknown_status_theme_color_is_grey(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="unknown"))
        assert payload["themeColor"] == "808080"

    def test_message_card_type(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        assert payload["@type"] == "MessageCard"

    def test_facts_include_pipeline_and_status(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        facts = payload["sections"][0]["facts"]
        fact_names = [f["name"] for f in facts]
        assert "Pipeline" in fact_names
        assert "Status" in fact_names

    def test_message_fact_included_when_present(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(message="pipeline stale"))
        facts = payload["sections"][0]["facts"]
        fact_names = [f["name"] for f in facts]
        assert "Message" in fact_names

    def test_custom_title_prefix(self):
        backend = _make_backend(title_prefix="[PROD]")
        payload = backend._build_payload(_make_event())
        assert payload["summary"].startswith("[PROD]")


class TestMSTeamsWebhookSend:
    def test_send_posts_to_webhook_url(self):
        backend = _make_backend()
        mock_resp = _mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=mock_resp) as m:
            backend.send(_make_event())
            m.assert_called_once()
            req = m.call_args[0][0]
            assert req.full_url == "https://example.webhook.office.com/xxx"

    def test_send_raises_on_non_200(self):
        backend = _make_backend()
        mock_resp = _mock_urlopen(400)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            with pytest.raises(RuntimeError, match="HTTP 400"):
                backend.send(_make_event())

    def test_payload_is_valid_json(self):
        backend = _make_backend()
        event = _make_event()
        payload = backend._build_payload(event)
        # Should not raise
        json.dumps(payload)
