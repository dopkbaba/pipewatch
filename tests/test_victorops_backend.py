"""Tests for the VictorOps alert backend."""
from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch
from contextlib import contextmanager

from pipewatch.alerting import AlertEvent
from pipewatch.backends.victorops import VictorOpsAlertBackend, VictorOpsAlertConfig


def _make_backend(
    api_key: str = "test-api-key",
    routing_key: str = "default",
    endpoint: str = "https://alert.victorops.com/integrations/generic/20131114/alert",
) -> VictorOpsAlertBackend:
    cfg = VictorOpsAlertConfig(
        routing_key=routing_key,
        api_key=api_key,
        rest_endpoint=endpoint,
    )
    return VictorOpsAlertBackend(cfg)


def _make_event(
    pipeline_id: str = "pipe-1",
    status: str = "critical",
    message: str = "Pipeline is stale",
) -> AlertEvent:
    return AlertEvent(pipeline_id=pipeline_id, status=status, message=message)


@contextmanager
 def _mock_urlopen(status: int = 200):
    resp = MagicMock()
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    with patch("urllib.request.urlopen", return_value=resp) as mock_open:
        yield mock_open


class TestVictorOpsPayload:
    def test_critical_maps_to_critical_message_type(self):
        backend = _make_backend()
        event = _make_event(status="critical")
        payload = backend._build_payload(event)
        assert payload["message_type"] == "CRITICAL"

    def test_warning_maps_to_warning_message_type(self):
        backend = _make_backend()
        event = _make_event(status="warning")
        payload = backend._build_payload(event)
        assert payload["message_type"] == "WARNING"

    def test_ok_maps_to_recovery_message_type(self):
        backend = _make_backend()
        event = _make_event(status="ok")
        payload = backend._build_payload(event)
        assert payload["message_type"] == "RECOVERY"

    def test_unknown_maps_to_warning_message_type(self):
        backend = _make_backend()
        event = _make_event(status="unknown")
        payload = backend._build_payload(event)
        assert payload["message_type"] == "WARNING"

    def test_entity_id_is_pipeline_id(self):
        backend = _make_backend()
        event = _make_event(pipeline_id="my-pipeline")
        payload = backend._build_payload(event)
        assert payload["entity_id"] == "my-pipeline"

    def test_display_name_contains_status_and_pipeline(self):
        backend = _make_backend()
        event = _make_event(pipeline_id="etl-job", status="critical")
        payload = backend._build_payload(event)
        assert "CRITICAL" in payload["entity_display_name"]
        assert "etl-job" in payload["entity_display_name"]

    def test_state_message_matches_event_message(self):
        backend = _make_backend()
        event = _make_event(message="Something went wrong")
        payload = backend._build_payload(event)
        assert payload["state_message"] == "Something went wrong"

    def test_monitoring_tool_is_pipewatch(self):
        backend = _make_backend()
        event = _make_event()
        payload = backend._build_payload(event)
        assert payload["monitoring_tool"] == "pipewatch"


class TestVictorOpsAlertBackendSend:
    def test_send_posts_to_correct_url(self):
        backend = _make_backend(api_key="ak", routing_key="rk", endpoint="https://vo.example.com")
        event = _make_event()
        with _mock_urlopen(200) as mock_open:
            backend.send(event)
        call_args = mock_open.call_args
        req = call_args[0][0]
        assert "ak" in req.full_url
        assert "rk" in req.full_url

    def test_send_raises_on_non_200(self):
        import pytest
        backend = _make_backend()
        event = _make_event()
        with _mock_urlopen(500):
            with pytest.raises(RuntimeError, match="unexpected status"):
                backend.send(event)
