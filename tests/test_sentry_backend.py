"""Tests for the Sentry backend."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.sentry import SentryBackend


def _make_backend() -> SentryBackend:
    return SentryBackend(
        dsn="https://sentry.io",
        auth_token="tok_test",
        org_slug="acme",
        project_slug="etl",
        timeout=5,
    )


def _mock_response(payload) -> MagicMock:
    body = json.dumps(payload).encode()
    resp = MagicMock()
    resp.status = 200
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


# ---------------------------------------------------------------------------
class TestSentryBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        issues = [
            {"tags": [{"key": "pipeline_id", "value": "pipe_b"}], "count": 0},
            {"tags": [{"key": "pipeline_id", "value": "pipe_a"}], "count": 0},
        ]
        backend = _make_backend()
        with patch("urllib.request.urlopen", return_value=_mock_response(issues)):
            result = backend.list_pipelines()
        assert result == ["pipe_a", "pipe_b"]

    def test_returns_empty_when_no_pipeline_tag(self):
        issues = [{"tags": [{"key": "environment", "value": "prod"}], "count": 0}]
        backend = _make_backend()
        with patch("urllib.request.urlopen", return_value=_mock_response(issues)):
            result = backend.list_pipelines()
        assert result == []

    def test_raises_on_non_200(self):
        import urllib.error

        backend = _make_backend()
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.HTTPError(None, 401, "Unauthorized", {}, None),
        ):
            with pytest.raises(RuntimeError, match="401"):
                backend.list_pipelines()


# ---------------------------------------------------------------------------
class TestSentryBackendFetch:
    def test_returns_empty_metrics_when_no_issues(self):
        backend = _make_backend()
        with patch("urllib.request.urlopen", return_value=_mock_response([])):
            metrics = backend.fetch("pipe_x")
        assert metrics.pipeline_id == "pipe_x"
        assert metrics.last_run is None
        assert metrics.row_count is None
        assert metrics.error_count is None

    def test_parses_aware_datetime(self):
        issues = [
            {
                "tags": [
                    {"key": "pipeline_id", "value": "pipe_a"},
                    {"key": "last_run", "value": "2024-03-01T12:00:00Z"},
                    {"key": "row_count", "value": "500"},
                ],
                "count": 3,
            }
        ]
        backend = _make_backend()
        with patch("urllib.request.urlopen", return_value=_mock_response(issues)):
            metrics = backend.fetch("pipe_a")
        assert metrics.last_run == datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert metrics.row_count == 500
        assert metrics.error_count == 3

    def test_error_count_defaults_to_zero(self):
        issues = [
            {
                "tags": [{"key": "pipeline_id", "value": "pipe_a"}],
                "count": 0,
            }
        ]
        backend = _make_backend()
        with patch("urllib.request.urlopen", return_value=_mock_response(issues)):
            metrics = backend.fetch("pipe_a")
        assert metrics.error_count == 0
