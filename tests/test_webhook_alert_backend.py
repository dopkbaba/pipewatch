"""Tests for the generic webhook alert backend."""
from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.webhook_alert import WebhookAlertBackend, WebhookAlertConfig
from pipewatch.health import HealthStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_backend(
    url: str = "https://hooks.example.com/alert",
    method: str = "POST",
    headers: dict | None = None,
    timeout: int = 10,
    extra: dict | None = None,
) -> WebhookAlertBackend:
    return WebhookAlertBackend(
        WebhookAlertConfig(
            url=url,
            method=method,
            headers=headers or {},
            timeout=timeout,
            extra=extra or {},
        )
    )


def _make_event(
    pipeline_id: str = "pipe_a",
    status: HealthStatus = HealthStatus.CRITICAL,
    message: str = "Pipeline is stale",
    metric_value: float | None = None,
) -> AlertEvent:
    return AlertEvent(
        pipeline_id=pipeline_id,
        status=status,
        message=message,
        metric_value=metric_value,
    )


def _mock_urlopen(status: int = 200):
    """Return a context-manager mock whose .status == *status*."""
    resp = MagicMock()
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


# ---------------------------------------------------------------------------
# Payload construction
# ---------------------------------------------------------------------------

class TestWebhookAlertPayload:
    def test_payload_contains_pipeline_id(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(pipeline_id="etl_daily"))
        assert payload["pipeline_id"] == "etl_daily"

    def test_payload_status_is_upper(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status=HealthStatus.WARNING))
        assert payload["status"] == "WARNING"

    def test_payload_contains_message(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(message="Something broke"))
        assert payload["message"] == "Something broke"

    def test_metric_value_included_when_set(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(metric_value=3.14))
        assert payload["metric_value"] == pytest.approx(3.14)

    def test_metric_value_absent_when_none(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(metric_value=None))
        assert "metric_value" not in payload

    def test_extra_fields_merged_into_payload(self):
        backend = _make_backend(extra={"team": "data-eng", "env": "prod"})
        payload = backend._build_payload(_make_event())
        assert payload["team"] == "data-eng"
        assert payload["env"] == "prod"


# ---------------------------------------------------------------------------
# send()
# ---------------------------------------------------------------------------

class TestWebhookAlertSend:
    def test_sends_post_request(self):
        backend = _make_backend(url="https://hooks.example.com/alert")
        mock_resp = _mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            backend.send(_make_event())
        mock_open.assert_called_once()
        req = mock_open.call_args[0][0]
        assert req.get_method() == "POST"
        assert req.full_url == "https://hooks.example.com/alert"

    def test_content_type_header_set(self):
        backend = _make_backend()
        mock_resp = _mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            backend.send(_make_event())
        req = mock_open.call_args[0][0]
        assert req.get_header("Content-type") == "application/json"

    def test_custom_headers_forwarded(self):
        backend = _make_backend(headers={"X-Token": "secret"})
        mock_resp = _mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            backend.send(_make_event())
        req = mock_open.call_args[0][0]
        assert req.get_header("X-token") == "secret"

    def test_request_body_is_valid_json(self):
        backend = _make_backend()
        mock_resp = _mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            backend.send(_make_event(pipeline_id="pipe_x"))
        req = mock_open.call_args[0][0]
        body = json.loads(req.data.decode())
        assert body["pipeline_id"] == "pipe_x"

    def test_raises_on_4xx_response(self):
        backend = _make_backend()
        mock_resp = _mock_urlopen(400)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            with pytest.raises(RuntimeError, match="400"):
                backend.send(_make_event())

    def test_custom_http_method_used(self):
        backend = _make_backend(method="PUT")
        mock_resp = _mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            backend.send(_make_event())
        req = mock_open.call_args[0][0]
        assert req.get_method() == "PUT"
