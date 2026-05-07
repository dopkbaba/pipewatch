"""Tests for the Pushover alert backend."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.pushover import (
    PushoverAlertBackend,
    PushoverAlertConfig,
    _DEFAULT_PRIORITY_MAP,
)


def _make_backend(
    user_key: str = "user123",
    api_token: str = "token456",
    priority_map=None,
    timeout: int = 10,
) -> PushoverAlertBackend:
    cfg = PushoverAlertConfig(
        user_key=user_key,
        api_token=api_token,
        priority_map=priority_map or dict(_DEFAULT_PRIORITY_MAP),
        timeout=timeout,
    )
    return PushoverAlertBackend(cfg)


def _make_event(
    pipeline_id: str = "etl-main",
    status: str = "critical",
    message: str = "Pipeline stalled",
) -> AlertEvent:
    return AlertEvent(pipeline_id=pipeline_id, status=status, message=message)


def _mock_urlopen(status: int = 200):
    cm = MagicMock()
    cm.__enter__ = lambda s: s
    cm.__exit__ = MagicMock(return_value=False)
    cm.status = status
    cm.read.return_value = b"{}"
    return cm


class TestPushoverPayload:
    def test_title_contains_pipeline_id(self):
        backend = _make_backend()
        event = _make_event()
        payload = backend._build_payload(event)
        assert "etl-main" in payload["title"]

    def test_title_contains_status_upper(self):
        backend = _make_backend()
        event = _make_event(status="warning")
        payload = backend._build_payload(event)
        assert "WARNING" in payload["title"]

    def test_message_contains_event_message(self):
        backend = _make_backend()
        event = _make_event(message="Row count too low")
        payload = backend._build_payload(event)
        assert "Row count too low" in payload["message"]

    def test_critical_maps_to_priority_1(self):
        backend = _make_backend()
        event = _make_event(status="critical")
        payload = backend._build_payload(event)
        assert payload["priority"] == 1

    def test_ok_maps_to_priority_minus_1(self):
        backend = _make_backend()
        event = _make_event(status="ok")
        payload = backend._build_payload(event)
        assert payload["priority"] == -1

    def test_unknown_status_falls_back_to_zero(self):
        backend = _make_backend()
        event = _make_event(status="degraded")
        payload = backend._build_payload(event)
        assert payload["priority"] == 0

    def test_payload_contains_user_key(self):
        backend = _make_backend(user_key="myuser")
        event = _make_event()
        payload = backend._build_payload(event)
        assert payload["user"] == "myuser"

    def test_payload_contains_api_token(self):
        backend = _make_backend(api_token="mytoken")
        event = _make_event()
        payload = backend._build_payload(event)
        assert payload["token"] == "mytoken"

    def test_custom_priority_map_overrides(self):
        backend = _make_backend(priority_map={"critical": 2, "warning": 1, "ok": 0, "unknown": 0})
        event = _make_event(status="critical")
        payload = backend._build_payload(event)
        assert payload["priority"] == 2

    def test_send_calls_urlopen(self):
        backend = _make_backend()
        event = _make_event()
        mock_resp = _mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            backend.send(event)
            mock_open.assert_called_once()

    def test_send_raises_on_non_200(self):
        backend = _make_backend()
        event = _make_event()
        mock_resp = _mock_urlopen(400)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            with pytest.raises(RuntimeError, match="400"):
                backend.send(event)
