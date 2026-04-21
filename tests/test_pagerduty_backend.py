"""Tests for the PagerDuty alert backend."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.pagerduty import (
    PagerDutyAlertBackend,
    PagerDutyAlertConfig,
)


def _make_backend(key: str = "test-key") -> PagerDutyAlertBackend:
    return PagerDutyAlertBackend(PagerDutyAlertConfig(integration_key=key))


def _make_event(
    pipeline_id: str = "pipe-1",
    status: str = "CRITICAL",
    message: str = "pipeline is stale",
) -> AlertEvent:
    return AlertEvent(pipeline_id=pipeline_id, status=status, message=message)


class TestPagerDutyPayload:
    def test_routing_key_in_payload(self):
        backend = _make_backend("my-key-123")
        payload = backend._build_payload(_make_event())
        assert payload["routing_key"] == "my-key-123"

    def test_event_action_is_trigger(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        assert payload["event_action"] == "trigger"

    def test_critical_maps_to_pd_critical(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="CRITICAL"))
        assert payload["payload"]["severity"] == "critical"

    def test_warning_maps_to_pd_warning(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="WARNING"))
        assert payload["payload"]["severity"] == "warning"

    def test_ok_maps_to_pd_info(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="OK"))
        assert payload["payload"]["severity"] == "info"

    def test_summary_contains_pipeline_id(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(pipeline_id="etl-daily"))
        assert "etl-daily" in payload["payload"]["summary"]

    def test_summary_contains_status(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="CRITICAL"))
        assert "CRITICAL" in payload["payload"]["summary"]

    def test_custom_details_has_pipeline_id(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(pipeline_id="p1"))
        assert payload["payload"]["custom_details"]["pipeline_id"] == "p1"

    def test_source_default(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        assert payload["payload"]["source"] == "pipewatch"

    def test_custom_source(self):
        cfg = PagerDutyAlertConfig(integration_key="k", source="my-service")
        backend = PagerDutyAlertBackend(cfg)
        payload = backend._build_payload(_make_event())
        assert payload["payload"]["source"] == "my-service"


class TestPagerDutySend:
    def test_send_posts_to_events_api(self):
        backend = _make_backend()
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 202

        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            backend.send(_make_event())

        mock_open.assert_called_once()
        req = mock_open.call_args[0][0]
        assert "events.pagerduty.com" in req.full_url

    def test_send_raises_on_http_error(self):
        import urllib.error

        backend = _make_backend()
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.HTTPError(
                url="", code=400, msg="Bad Request", hdrs=None, fp=None
            ),
        ):
            with pytest.raises(RuntimeError, match="HTTP 400"):
                backend.send(_make_event())
