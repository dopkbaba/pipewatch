"""Tests for the SignalR backend."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.signalr import SignalRBackend


def _make_backend(**kwargs) -> SignalRBackend:
    return SignalRBackend(
        base_url=kwargs.get("base_url", "http://localhost:5000"),
        hub=kwargs.get("hub", "pipewatch"),
        timeout=kwargs.get("timeout", 5),
    )


def _mock_response(payload, status: int = 200):
    raw = json.dumps(payload).encode()
    resp = MagicMock()
    resp.status = status
    resp.read.return_value = raw
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


class TestSignalRBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend()
        payload = [{"id": "pipe_b"}, {"id": "pipe_a"}, {"id": "pipe_c"}]
        with patch("urllib.request.urlopen", return_value=_mock_response(payload)):
            result = backend.list_pipelines()
        assert result == ["pipe_a", "pipe_b", "pipe_c"]

    def test_raises_on_non_200(self):
        backend = _make_backend()
        with patch(
            "urllib.request.urlopen", return_value=_mock_response([], status=503)
        ):
            with pytest.raises(RuntimeError, match="HTTP 503"):
                backend.list_pipelines()


class TestSignalRBackendFetch:
    def test_returns_empty_metrics_when_no_data(self):
        backend = _make_backend()
        with patch("urllib.request.urlopen", return_value=_mock_response({})):
            m = backend.fetch("pipe_a")
        assert m.pipeline_id == "pipe_a"
        assert m.last_run is None
        assert m.row_count is None

    def test_parses_aware_datetime(self):
        backend = _make_backend()
        payload = {
            "last_run": "2024-03-15T12:00:00+00:00",
            "row_count": 500,
            "error_count": 2,
            "duration_seconds": 30.5,
        }
        with patch("urllib.request.urlopen", return_value=_mock_response(payload)):
            m = backend.fetch("pipe_a")
        assert m.last_run == datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert m.row_count == 500
        assert m.error_count == 2
        assert m.duration_seconds == 30.5

    def test_parses_naive_datetime_as_utc(self):
        backend = _make_backend()
        payload = {"last_run": "2024-03-15T08:30:00"}
        with patch("urllib.request.urlopen", return_value=_mock_response(payload)):
            m = backend.fetch("pipe_a")
        assert m.last_run is not None
        assert m.last_run.tzinfo == timezone.utc

    def test_uses_correct_hub_in_url(self):
        backend = _make_backend(hub="myhub")
        captured_urls = []

        def fake_urlopen(req, timeout=None):
            captured_urls.append(req.full_url)
            return _mock_response({})

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            backend.fetch("pipe_x")

        assert "/hubs/myhub/pipelines/pipe_x" in captured_urls[0]
