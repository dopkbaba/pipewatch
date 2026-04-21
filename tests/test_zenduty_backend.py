"""Tests for the Zenduty alert backend."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.zenduty import ZendutyAlertBackend, ZendutyAlertConfig


def _make_backend(
    api_key: str = "test-key",
    service_id: str = "svc-123",
) -> ZendutyAlertBackend:
    cfg = ZendutyAlertConfig(api_key=api_key, service_id=service_id)
    return ZendutyAlertBackend(cfg)


def _make_event(
    pipeline_id: str = "pipe-a",
    status: str = "CRITICAL",
    message: str = "pipeline stale",
) -> AlertEvent:
    return AlertEvent(pipeline_id=pipeline_id, status=status, message=message)


class TestZendutyPayload:
    def test_title_contains_pipeline_id(self):
        backend = _make_backend()
        event = _make_event(pipeline_id="etl-daily")
        payload = backend._build_payload(event)
        assert "etl-daily" in payload["title"]

    def test_title_contains_status_upper(self):
        backend = _make_backend()
        event = _make_event(status="warning")
        payload = backend._build_payload(event)
        assert "WARNING" in payload["title"]

    def test_urgency_is_high_for_critical(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="CRITICAL"))
        assert payload["urgency"] == 1

    def test_urgency_is_low_for_warning(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="WARNING"))
        assert payload["urgency"] == 2

    def test_message_in_payload(self):
        backend = _make_backend()
        event = _make_event(message="row count dropped")
        payload = backend._build_payload(event)
        assert payload["message"] == "row count dropped"

    def test_service_id_in_payload(self):
        backend = _make_backend(service_id="svc-xyz")
        payload = backend._build_payload(_make_event())
        assert payload["service"] == "svc-xyz"

    def test_fallback_message_when_none(self):
        backend = _make_backend()
        event = AlertEvent(pipeline_id="p1", status="CRITICAL", message=None)
        payload = backend._build_payload(event)
        assert "p1" in payload["message"]


class TestZendutyBackendSend:
    def _fake_response(self, status: int = 201):
        resp = MagicMock()
        resp.status = status
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    def test_send_posts_to_incidents_url(self):
        backend = _make_backend()
        event = _make_event()
        fake_resp = self._fake_response(201)
        with patch("urllib.request.urlopen", return_value=fake_resp) as mock_open:
            backend.send(event)
        call_args = mock_open.call_args
        req = call_args[0][0]
        assert "incidents" in req.full_url

    def test_send_includes_auth_header(self):
        backend = _make_backend(api_key="secret-key")
        event = _make_event()
        fake_resp = self._fake_response(201)
        with patch("urllib.request.urlopen", return_value=fake_resp) as mock_open:
            backend.send(event)
        req = mock_open.call_args[0][0]
        assert req.get_header("Authorization") == "Token secret-key"

    def test_raises_on_http_error(self):
        import urllib.error
        backend = _make_backend()
        event = _make_event()
        err = urllib.error.HTTPError(url="", code=403, msg="Forbidden", hdrs=None, fp=None)
        with patch("urllib.request.urlopen", side_effect=err):
            with pytest.raises(RuntimeError, match="403"):
                backend.send(event)
