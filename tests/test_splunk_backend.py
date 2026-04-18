"""Tests for the Splunk backend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.splunk import SplunkBackend


def _make_backend() -> SplunkBackend:
    return SplunkBackend(base_url="http://splunk:8089", token="tok", index="pw")


def _mock_response(lines: list[str], status: int = 200):
    resp = MagicMock()
    resp.status_code = status
    resp.text = "\n".join(lines)
    return resp


class TestSplunkBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        import json
        lines = [
            json.dumps({"result": {"pipeline_id": "pipe_b"}}),
            json.dumps({"result": {"pipeline_id": "pipe_a"}}),
        ]
        backend = _make_backend()
        backend._session.post = MagicMock(return_value=_mock_response(lines))
        assert backend.list_pipelines() == ["pipe_a", "pipe_b"]

    def test_raises_on_non_200(self):
        backend = _make_backend()
        backend._session.post = MagicMock(return_value=_mock_response([], status=401))
        with pytest.raises(RuntimeError, match="401"):
            backend.list_pipelines()


class TestSplunkBackendFetch:
    def test_returns_empty_metrics_when_no_results(self):
        backend = _make_backend()
        backend._session.post = MagicMock(return_value=_mock_response([]))
        m = backend.fetch("pipe_x")
        assert m.pipeline_id == "pipe_x"
        assert m.last_run is None
        assert m.row_count is None
        assert m.error_rate is None

    def test_parses_aware_datetime(self):
        import json
        row = {
            "pipeline_id": "pipe_a",
            "last_run": "2024-03-01T12:00:00",
            "row_count": "500",
            "error_rate": "0.02",
        }
        lines = [json.dumps({"result": row})]
        backend = _make_backend()
        backend._session.post = MagicMock(return_value=_mock_response(lines))
        m = backend.fetch("pipe_a")
        assert m.last_run == datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert m.row_count == 500
        assert m.error_rate == pytest.approx(0.02)

    def test_parses_already_aware_datetime(self):
        import json
        row = {
            "pipeline_id": "pipe_b",
            "last_run": "2024-03-01T12:00:00+00:00",
            "row_count": "100",
            "error_rate": "0.0",
        }
        lines = [json.dumps({"result": row})]
        backend = _make_backend()
        backend._session.post = MagicMock(return_value=_mock_response(lines))
        m = backend.fetch("pipe_b")
        assert m.last_run.tzinfo is not None


class TestSplunkRegister:
    def test_splunk_backend_is_registered(self):
        import pipewatch.backends.splunk_register  # noqa: F401
        from pipewatch.backends import get_backend
        backend = get_backend("splunk", {})
        assert isinstance(backend, SplunkBackend)

    def test_factory_passes_custom_config(self):
        from pipewatch.backends import get_backend
        backend = get_backend("splunk", {"base_url": "http://custom:9089", "index": "etl"})
        assert backend._base_url == "http://custom:9089"
        assert backend._index == "etl"
