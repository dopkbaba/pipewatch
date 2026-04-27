"""Tests for the Linear alert backend."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.linear import LinearAlertBackend, LinearAlertConfig
from pipewatch.alerting import AlertEvent
from pipewatch.health import HealthStatus


def _make_backend(**kwargs) -> LinearAlertBackend:
    cfg = LinearAlertConfig(
        api_key="lin_api_testkey",
        team_id="TEAM-1",
        **kwargs,
    )
    return LinearAlertBackend(cfg)


def _make_event(status: str = "critical", message: str = "") -> AlertEvent:
    return AlertEvent(pipeline_id="etl-orders", status=status, message=message)


def _mock_urlopen(success: bool = True):
    response_body = json.dumps(
        {"data": {"issueCreate": {"success": success, "issue": {"id": "abc", "identifier": "ENG-42", "url": "https://linear.app/t/ENG-42"}}}}
    ).encode()
    cm = MagicMock()
    cm.__enter__ = lambda s: s
    cm.__exit__ = MagicMock(return_value=False)
    cm.read.return_value = response_body
    return cm


class TestLinearPayload:
    def test_title_contains_pipeline_id(self):
        backend = _make_backend()
        event = _make_event()
        payload = backend._build_payload(event)
        assert "etl-orders" in payload["variables"]["input"]["title"]

    def test_title_contains_status_upper(self):
        backend = _make_backend()
        event = _make_event(status="warning")
        payload = backend._build_payload(event)
        assert "WARNING" in payload["variables"]["input"]["title"]

    def test_critical_maps_to_urgent_priority(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="critical"))
        assert payload["variables"]["input"]["priority"] == 1

    def test_warning_maps_to_high_priority(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="warning"))
        assert payload["variables"]["input"]["priority"] == 2

    def test_ok_maps_to_no_priority(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(status="ok"))
        assert payload["variables"]["input"]["priority"] == 4

    def test_description_contains_message(self):
        backend = _make_backend()
        event = _make_event(message="row count below threshold")
        payload = backend._build_payload(event)
        assert "row count below threshold" in payload["variables"]["input"]["description"]

    def test_label_id_included_when_set(self):
        backend = _make_backend(label_id="LABEL-99")
        payload = backend._build_payload(_make_event())
        assert payload["variables"]["input"]["labelIds"] == ["LABEL-99"]

    def test_label_id_absent_when_not_set(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        assert "labelIds" not in payload["variables"]["input"]

    def test_assignee_id_included_when_set(self):
        backend = _make_backend(assignee_id="USER-7")
        payload = backend._build_payload(_make_event())
        assert payload["variables"]["input"]["assigneeId"] == "USER-7"

    def test_team_id_in_payload(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        assert payload["variables"]["input"]["teamId"] == "TEAM-1"


class TestLinearSend:
    def test_send_calls_linear_api(self):
        backend = _make_backend()
        mock_resp = _mock_urlopen(success=True)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            backend.send(_make_event())  # should not raise

    def test_send_raises_on_api_failure(self):
        backend = _make_backend()
        fail_body = json.dumps({"data": {"issueCreate": {"success": False}}}).encode()
        cm = MagicMock()
        cm.__enter__ = lambda s: s
        cm.__exit__ = MagicMock(return_value=False)
        cm.read.return_value = fail_body
        with patch("urllib.request.urlopen", return_value=cm):
            with pytest.raises(RuntimeError, match="Linear issue creation failed"):
                backend.send(_make_event())

    def test_extra_headers_forwarded(self):
        backend = _make_backend(extra_headers={"X-Custom": "yes"})
        mock_resp = _mock_urlopen(success=True)
        captured = {}

        def fake_urlopen(req, timeout):
            captured["headers"] = dict(req.headers)
            return mock_resp

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            backend.send(_make_event())

        assert captured["headers"].get("X-custom") == "yes"
