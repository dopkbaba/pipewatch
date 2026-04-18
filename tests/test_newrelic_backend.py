"""Tests for the New Relic backend."""
from __future__ import annotations

from datetime import timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.newrelic import NewRelicBackend


def _make_backend() -> NewRelicBackend:
    return NewRelicBackend(account_id="12345", api_key="test-key")


def _mock_response(payload: dict, status: int = 200):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = payload
    resp.text = ""
    return resp


class TestNewRelicBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        payload = {"results": [{"members": ["pipe_b", "pipe_a"]}]}
        with patch("requests.get", return_value=_mock_response(payload)):
            ids = _make_backend().list_pipelines()
        assert ids == ["pipe_a", "pipe_b"]

    def test_raises_on_non_200(self):
        with patch("requests.get", return_value=_mock_response({}, status=403)):
            with pytest.raises(RuntimeError, match="403"):
                _make_backend().list_pipelines()

    def test_empty_when_no_members(self):
        payload = {"results": [{"members": []}]}
        with patch("requests.get", return_value=_mock_response(payload)):
            assert _make_backend().list_pipelines() == []


class TestNewRelicBackendFetch:
    def test_returns_empty_metrics_when_no_results(self):
        with patch("requests.get", return_value=_mock_response({"results": []})):
            m = _make_backend().fetch("pipe_x")
        assert m.pipeline_id == "pipe_x"
        assert m.last_run is None
        assert m.row_count is None
        assert m.error_rate is None

    def test_parses_aware_datetime(self):
        ts_ms = 1_700_000_000_000
        payload = {
            "results": [{
                "latest.last_run": ts_ms,
                "latest.row_count": 500,
                "latest.error_rate": 0.01,
            }]
        }
        with patch("requests.get", return_value=_mock_response(payload)):
            m = _make_backend().fetch("pipe_a")
        assert m.last_run is not None
        assert m.last_run.tzinfo == timezone.utc
        assert m.row_count == 500
        assert m.error_rate == pytest.approx(0.01)

    def test_registers_via_factory(self):
        import pipewatch.backends.newrelic_register  # noqa: F401
        from pipewatch.backends import get_backend
        with patch("requests.get"):
            backend = get_backend("newrelic", {"account_id": "1", "api_key": "k"})
        assert isinstance(backend, NewRelicBackend)
