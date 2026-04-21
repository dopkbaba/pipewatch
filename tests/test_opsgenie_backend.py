"""Tests for the OpsGenie alert backend."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.opsgenie import OpsGenieAlertBackend, OpsGenieAlertConfig


def _make_backend(
    api_key: str = "test-key",
    tags: list | None = None,
    priority: str = "P3",
    responders: list | None = None,
) -> OpsGenieAlertBackend:
    cfg = OpsGenieAlertConfig(
        api_key=api_key,
        tags=tags or [],
        priority=priority,
        responders=responders or [],
    )
    return OpsGenieAlertBackend(cfg)


def _make_event(
    pipeline_id: str = "etl-main",
    status: str = "CRITICAL",
    message: str = "Pipeline is stale",
    details: dict | None = None,
) -> AlertEvent:
    return AlertEvent(
        pipeline_id=pipeline_id,
        status=status,
        message=message,
        details=details or {},
    )


class TestOpsGeniePayload:
    def test_message_contains_pipeline_id(self):
        backend = _make_backend()
        event = _make_event(pipeline_id="my-pipeline")
        payload = backend._build_payload(event)
        assert "my-pipeline" in payload["message"]

    def test_message_contains_status_upper(self):
        backend = _make_backend()
        event = _make_event(status="WARNING")
        payload = backend._build_payload(event)
        assert "WARNING" in payload["message"]

    def test_critical_maps_to_p1(self):
        backend = _make_backend()
        event = _make_event(status="CRITICAL")
        payload = backend._build_payload(event)
        assert payload["priority"] == "P1"

    def test_warning_maps_to_p3(self):
        backend = _make_backend()
        event = _make_event(status="WARNING")
        payload = backend._build_payload(event)
        assert payload["priority"] == "P3"

    def test_ok_maps_to_p5(self):
        backend = _make_backend()
        event = _make_event(status="OK")
        payload = backend._build_payload(event)
        assert payload["priority"] == "P5"

    def test_tags_included_in_payload(self):
        backend = _make_backend(tags=["etl", "production"])
        event = _make_event()
        payload = backend._build_payload(event)
        assert "etl" in payload["tags"]
        assert "production" in payload["tags"]

    def test_responders_included_when_set(self):
        responders = [{"type": "team", "name": "ops-team"}]
        backend = _make_backend(responders=responders)
        event = _make_event()
        payload = backend._build_payload(event)
        assert payload["responders"] == responders

    def test_responders_absent_when_empty(self):
        backend = _make_backend(responders=[])
        event = _make_event()
        payload = backend._build_payload(event)
        assert "responders" not in payload

    def test_details_stringified(self):
        backend = _make_backend()
        event = _make_event(details={"rows": 42, "lag_seconds": 120})
        payload = backend._build_payload(event)
        assert payload["details"]["rows"] == "42"
        assert payload["details"]["lag_seconds"] == "120"

    def test_alias_uses_pipeline_id(self):
        backend = _make_backend()
        event = _make_event(pipeline_id="orders-pipeline")
        payload = backend._build_payload(event)
        assert payload["alias"] == "pipewatch-orders-pipeline"


class TestOpsGenieSend:
    def test_send_posts_to_opsgenie(self):
        backend = _make_backend(api_key="secret")
        event = _make_event()
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 202
        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            backend.send(event)
        mock_open.assert_called_once()
        req = mock_open.call_args[0][0]
        assert req.get_header("Authorization") == "GenieKey secret"
        assert req.get_header("Content-type") == "application/json"
        body = json.loads(req.data)
        assert "message" in body

    def test_send_raises_on_http_error(self):
        import urllib.error
        backend = _make_backend()
        event = _make_event()
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.HTTPError(
                url=None, code=403, msg="Forbidden", hdrs=None, fp=None
            ),
        ):
            with pytest.raises(RuntimeError, match="OpsGenie request failed"):
                backend.send(event)
