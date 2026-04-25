"""Tests for the Statuspage.io alert backend."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.statuspage import (
    StatuspageAlertBackend,
    StatuspageAlertConfig,
    _STATUS_MAP,
)


def _make_backend(
    api_key="key-abc",
    page_id="page123",
    component_id="comp456",
    base_url="https://api.statuspage.io/v1",
    timeout=5,
) -> StatuspageAlertBackend:
    cfg = StatuspageAlertConfig(
        api_key=api_key,
        page_id=page_id,
        component_id=component_id,
        base_url=base_url,
        timeout=timeout,
    )
    return StatuspageAlertBackend(cfg)


def _make_event(status="critical", pipeline_id="pipe-1", message="down"):
    return AlertEvent(
        pipeline_id=pipeline_id,
        status=status,
        message=message,
    )


class TestStatuspagePayload:
    def test_critical_maps_to_major_outage(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="critical"))
        assert payload["component"]["status"] == "major_outage"

    def test_warning_maps_to_degraded_performance(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="warning"))
        assert payload["component"]["status"] == "degraded_performance"

    def test_ok_maps_to_operational(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="ok"))
        assert payload["component"]["status"] == "operational"

    def test_unknown_maps_to_under_maintenance(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="unknown"))
        assert payload["component"]["status"] == "under_maintenance"

    def test_description_contains_pipeline_id(self):
        backend = _make_backend()
        payload = backend._build_payload(
            _make_event(pipeline_id="my-pipeline")
        )
        assert "my-pipeline" in payload["component"]["description"]

    def test_description_contains_message(self):
        backend = _make_backend()
        payload = backend._build_payload(
            _make_event(message="latency spike")
        )
        assert "latency spike" in payload["component"]["description"]


class TestStatuspageSend:
    def _mock_urlopen(self, status=200):
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=MagicMock(status=status))
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    def test_sends_patch_request(self):
        backend = _make_backend()
        event = _make_event()
        ctx = self._mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=ctx) as mock_open:
            with patch("urllib.request.Request") as mock_req:
                backend.send(event)
                mock_req.assert_called_once()
                _, kwargs = mock_req.call_args
                assert kwargs.get("method") == "PATCH"

    def test_raises_on_non_200(self):
        backend = _make_backend()
        event = _make_event()
        ctx = self._mock_urlopen(500)
        with patch("urllib.request.urlopen", return_value=ctx):
            with patch("urllib.request.Request"):
                with pytest.raises(RuntimeError, match="500"):
                    backend.send(event)

    def test_authorization_header_contains_api_key(self):
        backend = _make_backend(api_key="secret-key")
        event = _make_event()
        ctx = self._mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=ctx):
            with patch("urllib.request.Request") as mock_req:
                backend.send(event)
                _, kwargs = mock_req.call_args
                assert "secret-key" in kwargs["headers"]["Authorization"]
