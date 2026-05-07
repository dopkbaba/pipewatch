"""Tests for the ntfy alert backend."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.ntfy import NtfyAlertBackend, NtfyAlertConfig


def _make_backend(**kwargs) -> NtfyAlertBackend:
    return NtfyAlertBackend(NtfyAlertConfig(**kwargs))


def _make_event(status: str = "critical", message: str = "something broke") -> AlertEvent:
    return AlertEvent(pipeline_id="pipe-1", status=status, message=message)


def _mock_urlopen(status: int = 200):
    resp = MagicMock()
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return patch("urllib.request.urlopen", return_value=resp)


class TestNtfyPayload:
    def test_title_contains_pipeline_id(self):
        backend = _make_backend()
        event = _make_event()
        payload = backend._build_payload(event)
        assert "pipe-1" in payload["title"]

    def test_title_contains_status_upper(self):
        backend = _make_backend()
        event = _make_event(status="warning")
        payload = backend._build_payload(event)
        assert "WARNING" in payload["title"]

    def test_topic_in_payload(self):
        backend = _make_backend(topic="my-topic")
        event = _make_event()
        payload = backend._build_payload(event)
        assert payload["topic"] == "my-topic"

    def test_critical_priority_is_5_by_default(self):
        backend = _make_backend()
        event = _make_event(status="critical")
        payload = backend._build_payload(event)
        assert payload["priority"] == 5

    def test_warning_priority_is_3_by_default(self):
        backend = _make_backend()
        event = _make_event(status="warning")
        payload = backend._build_payload(event)
        assert payload["priority"] == 3

    def test_custom_priority_map_is_respected(self):
        backend = _make_backend(priority_map={"critical": 4, "warning": 2, "ok": 1, "unknown": 1})
        event = _make_event(status="critical")
        payload = backend._build_payload(event)
        assert payload["priority"] == 4

    def test_unknown_status_falls_back_to_3(self):
        backend = _make_backend(priority_map={})
        event = _make_event(status="degraded")
        payload = backend._build_payload(event)
        assert payload["priority"] == 3

    def test_tags_include_status(self):
        backend = _make_backend()
        event = _make_event(status="ok")
        payload = backend._build_payload(event)
        assert "ok" in payload["tags"]
        assert "pipewatch" in payload["tags"]

    def test_custom_message_is_used(self):
        backend = _make_backend()
        event = _make_event(message="custom msg")
        payload = backend._build_payload(event)
        assert payload["message"] == "custom msg"


class TestNtfySend:
    def test_send_posts_to_server(self):
        backend = _make_backend(server="https://ntfy.example.com", topic="alerts")
        event = _make_event()
        with _mock_urlopen(200) as mock_open:
            backend.send(event)
        mock_open.assert_called_once()
        req = mock_open.call_args[0][0]
        assert req.full_url == "https://ntfy.example.com"
        body = json.loads(req.data)
        assert body["topic"] == "alerts"

    def test_send_raises_on_4xx(self):
        backend = _make_backend()
        event = _make_event()
        with _mock_urlopen(400):
            with pytest.raises(RuntimeError, match="ntfy returned HTTP 400"):
                backend.send(event)


class TestNtfyRegister:
    def test_factory_creates_backend_with_defaults(self):
        from pipewatch.backends.ntfy_register import _factory
        backend = _factory()
        assert isinstance(backend, NtfyAlertBackend)
        assert backend._cfg.server == "https://ntfy.sh"
        assert backend._cfg.topic == "pipewatch"

    def test_factory_passes_custom_config(self):
        from pipewatch.backends.ntfy_register import _factory
        backend = _factory({"server": "https://my.ntfy", "topic": "ops", "timeout": 5})
        assert backend._cfg.server == "https://my.ntfy"
        assert backend._cfg.topic == "ops"
        assert backend._cfg.timeout == 5
