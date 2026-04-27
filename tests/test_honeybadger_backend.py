"""Tests for the Honeybadger Insights backend."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.honeybadger import HoneybadgerBackend


def _make_backend() -> HoneybadgerBackend:
    return HoneybadgerBackend(
        api_key="test-key",
        project_id="proj-1",
        event_type="pipeline_health",
        timeout=5,
    )


def _mock_response(payload: dict) -> MagicMock:
    body = json.dumps(payload).encode()
    resp = MagicMock()
    resp.status = 200
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


class TestHoneybadgerBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend()
        payload = {
            "results": [
                {"pipeline_id": "pipe-b", "last_run": None},
                {"pipeline_id": "pipe-a", "last_run": None},
                {"pipeline_id": "pipe-b", "last_run": None},  # duplicate
            ]
        }
        with patch(
            "pipewatch.backends.honeybadger.urllib_request.urlopen",
            return_value=_mock_response(payload),
        ):
            result = backend.list_pipelines()
        assert result == ["pipe-a", "pipe-b"]

    def test_returns_empty_when_no_results(self):
        backend = _make_backend()
        with patch(
            "pipewatch.backends.honeybadger.urllib_request.urlopen",
            return_value=_mock_response({"results": []}),
        ):
            result = backend.list_pipelines()
        assert result == []

    def test_raises_on_non_200(self):
        backend = _make_backend()
        resp = _mock_response({})
        resp.status = 403
        with patch(
            "pipewatch.backends.honeybadger.urllib_request.urlopen",
            return_value=resp,
        ):
            with pytest.raises(RuntimeError, match="403"):
                backend.list_pipelines()


class TestHoneybadgerBackendFetch:
    def test_returns_empty_metrics_when_no_results(self):
        backend = _make_backend()
        with patch(
            "pipewatch.backends.honeybadger.urllib_request.urlopen",
            return_value=_mock_response({"results": []}),
        ):
            metrics = backend.fetch("pipe-x")
        assert metrics.pipeline_id == "pipe-x"
        assert metrics.last_run is None
        assert metrics.row_count is None

    def test_parses_aware_datetime(self):
        backend = _make_backend()
        payload = {
            "results": [
                {
                    "pipeline_id": "pipe-a",
                    "last_run": "2024-03-15T12:00:00Z",
                    "row_count": 500,
                    "error_count": 2,
                    "duration_seconds": 45.0,
                }
            ]
        }
        with patch(
            "pipewatch.backends.honeybadger.urllib_request.urlopen",
            return_value=_mock_response(payload),
        ):
            metrics = backend.fetch("pipe-a")
        assert metrics.last_run == datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert metrics.row_count == 500
        assert metrics.error_count == 2
        assert metrics.duration_seconds == 45.0

    def test_naive_datetime_becomes_utc(self):
        backend = _make_backend()
        payload = {
            "results": [{"pipeline_id": "p", "last_run": "2024-01-01T08:30:00"}]
        }
        with patch(
            "pipewatch.backends.honeybadger.urllib_request.urlopen",
            return_value=_mock_response(payload),
        ):
            metrics = backend.fetch("p")
        assert metrics.last_run is not None
        assert metrics.last_run.tzinfo == timezone.utc
