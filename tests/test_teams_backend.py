"""Tests for the Microsoft Teams alert backend."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.teams import TeamsAlertBackend, TeamsAlertConfig


def _make_backend(
    webhook_url: str = "https://teams.example.com/webhook",
    mention_on_critical: str | None = None,
) -> TeamsAlertBackend:
    cfg = TeamsAlertConfig(
        webhook_url=webhook_url,
        mention_on_critical=mention_on_critical,
    )
    return TeamsAlertBackend(cfg)


def _make_event(
    pipeline_id: str = "pipe-1",
    status: str = "critical",
    message: str = "Pipeline is stale",
) -> AlertEvent:
    return AlertEvent(pipeline_id=pipeline_id, status=status, message=message)


class TestTeamsPayload:
    def test_summary_contains_pipeline_id(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(pipeline_id="my-pipe"))
        assert "my-pipe" in payload["summary"]

    def test_summary_contains_status_upper(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="warning"))
        assert "WARNING" in payload["summary"]

    def test_critical_color_is_red(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="critical"))
        assert payload["themeColor"] == "FF0000"

    def test_warning_color_is_orange(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="warning"))
        assert payload["themeColor"] == "FFA500"

    def test_ok_color_is_green(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="ok"))
        assert payload["themeColor"] == "00AA00"

    def test_mention_prepended_on_critical(self):
        backend = _make_backend(mention_on_critical="<at>oncall</at>")
        payload = backend._build_payload(_make_event(status="critical"))
        text = payload["sections"][0]["activityText"]
        assert text.startswith("<at>oncall</at>")

    def test_mention_not_added_on_warning(self):
        backend = _make_backend(mention_on_critical="<at>oncall</at>")
        payload = backend._build_payload(_make_event(status="warning"))
        text = payload["sections"][0]["activityText"]
        assert "<at>oncall</at>" not in text

    def test_facts_include_pipeline_and_status(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(pipeline_id="p1", status="ok"))
        facts = payload["sections"][0]["facts"]
        names = {f["name"]: f["value"] for f in facts}
        assert names["Pipeline"] == "p1"
        assert names["Status"] == "OK"


class TestTeamsAlertBackendSend:
    def _mock_urlopen(self, status: int = 200):
        resp = MagicMock()
        resp.status = status
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    def test_sends_post_request(self):
        backend = _make_backend()
        resp = self._mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=resp) as mock_open:
            backend.send(_make_event())
        mock_open.assert_called_once()
        req = mock_open.call_args[0][0]
        assert req.method == "POST"
        assert req.full_url == "https://teams.example.com/webhook"

    def test_payload_is_valid_json(self):
        backend = _make_backend()
        resp = self._mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=resp) as mock_open:
            backend.send(_make_event())
        req = mock_open.call_args[0][0]
        parsed = json.loads(req.data.decode())
        assert parsed["@type"] == "MessageCard"

    def test_raises_on_non_200(self):
        backend = _make_backend()
        resp = self._mock_urlopen(500)
        with patch("urllib.request.urlopen", return_value=resp):
            with pytest.raises(RuntimeError, match="HTTP 500"):
                backend.send(_make_event())
