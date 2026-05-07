"""Tests for the Gotify alert backend."""
from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.gotify import GotifyAlertBackend, GotifyAlertConfig


def _make_backend(priority_map=None):
    cfg = GotifyAlertConfig(
        url="https://gotify.example.com",
        token="secret-token",
        priority_map=priority_map
        or {"critical": 9, "warning": 5, "ok": 1, "unknown": 3},
    )
    return GotifyAlertBackend(cfg)


def _make_event(status="critical", reason="pipeline stalled"):
    return AlertEvent(pipeline_id="etl-prod", status=status, reason=reason)


def _mock_urlopen(status=200):
    resp = MagicMock()
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


class TestGotifyPayload:
    def test_title_contains_pipeline_id(self):
        b = _make_backend()
        payload = b._build_payload(_make_event())
        assert "etl-prod" in payload["title"]

    def test_title_contains_status_upper(self):
        b = _make_backend()
        payload = b._build_payload(_make_event(status="warning"))
        assert "WARNING" in payload["title"]

    def test_message_contains_reason(self):
        b = _make_backend()
        payload = b._build_payload(_make_event(reason="row count too low"))
        assert "row count too low" in payload["message"]

    def test_critical_maps_to_priority_9(self):
        b = _make_backend()
        payload = b._build_payload(_make_event(status="critical"))
        assert payload["priority"] == 9

    def test_warning_maps_to_priority_5(self):
        b = _make_backend()
        payload = b._build_payload(_make_event(status="warning"))
        assert payload["priority"] == 5

    def test_ok_maps_to_priority_1(self):
        b = _make_backend()
        payload = b._build_payload(_make_event(status="ok"))
        assert payload["priority"] == 1

    def test_unknown_status_falls_back_to_3(self):
        b = _make_backend()
        payload = b._build_payload(_make_event(status="banana"))
        assert payload["priority"] == 3

    def test_send_posts_to_message_endpoint(self):
        b = _make_backend()
        with patch("urllib.request.urlopen", return_value=_mock_urlopen()) as mock_open:
            b.send(_make_event())
        req = mock_open.call_args[0][0]
        assert req.full_url == "https://gotify.example.com/message"
        assert req.get_header("X-gotify-key") == "secret-token"
        assert req.get_header("Content-type") == "application/json"

    def test_send_raises_on_non_200(self):
        b = _make_backend()
        with patch(
            "urllib.request.urlopen", return_value=_mock_urlopen(status=403)
        ):
            with pytest.raises(RuntimeError, match="403"):
                b.send(_make_event())

    def test_send_payload_is_valid_json(self):
        b = _make_backend()
        captured = {}

        def fake_open(req, timeout):
            captured["body"] = req.data
            return _mock_urlopen()

        with patch("urllib.request.urlopen", side_effect=fake_open):
            b.send(_make_event())

        parsed = json.loads(captured["body"])
        assert "title" in parsed
        assert "message" in parsed
        assert "priority" in parsed
