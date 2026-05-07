"""Tests for the Apprise alert backend."""
from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.apprise import AppriseAlertBackend, AppriseAlertConfig
from pipewatch.health import HealthStatus


def _make_backend(**kwargs) -> AppriseAlertBackend:
    return AppriseAlertBackend(AppriseAlertConfig(**kwargs))


def _make_event(status: str = HealthStatus.CRITICAL, message: str = "too slow") -> AlertEvent:
    return AlertEvent(pipeline_id="etl-prod", status=status, message=message)


def _mock_urlopen(status: int = 200):
    resp = MagicMock()
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return patch("pipewatch.backends.apprise.urlopen", return_value=resp)


class TestApprisePayload:
    def test_title_contains_pipeline_id(self):
        backend = _make_backend()
        event = _make_event()
        payload = backend._build_payload(event)
        assert "etl-prod" in payload["title"]

    def test_title_contains_status_upper(self):
        backend = _make_backend()
        event = _make_event(status=HealthStatus.WARNING)
        payload = backend._build_payload(event)
        assert "WARNING" in payload["title"]

    def test_body_contains_pipeline_id(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        assert "etl-prod" in payload["body"]

    def test_body_contains_message(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(message="row count mismatch"))
        assert "row count mismatch" in payload["body"]

    def test_body_omits_detail_when_message_empty(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(message=""))
        assert "Detail" not in payload["body"]

    def test_urls_included_when_configured(self):
        backend = _make_backend(urls="slack://token/channel")
        payload = backend._build_payload(_make_event())
        assert payload["urls"] == "slack://token/channel"

    def test_urls_omitted_when_empty(self):
        backend = _make_backend(urls="")
        payload = backend._build_payload(_make_event())
        assert "urls" not in payload

    def test_critical_emoji_in_title(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status=HealthStatus.CRITICAL))
        assert "\U0001f6a8" in payload["title"]

    def test_ok_emoji_in_title(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status=HealthStatus.OK))
        assert "\u2705" in payload["title"]


class TestAppriseAlertBackendSend:
    def test_sends_post_request(self):
        backend = _make_backend(api_url="http://apprise:8000/notify")
        with _mock_urlopen(200) as mock_open:
            backend.send(_make_event())
        mock_open.assert_called_once()
        req = mock_open.call_args[0][0]
        assert req.get_method() == "POST"
        assert req.full_url == "http://apprise:8000/notify"

    def test_sends_json_body(self):
        backend = _make_backend()
        with _mock_urlopen() as mock_open:
            backend.send(_make_event())
        req = mock_open.call_args[0][0]
        body = json.loads(req.data)
        assert "title" in body
        assert "body" in body

    def test_raises_on_non_200(self):
        backend = _make_backend()
        with _mock_urlopen(500):
            with pytest.raises(RuntimeError, match="HTTP 500"):
                backend.send(_make_event())
