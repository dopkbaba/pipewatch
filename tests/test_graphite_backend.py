"""Tests for the Graphite backend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.graphite import GraphiteBackend


def _make_backend(**kwargs) -> GraphiteBackend:
    return GraphiteBackend(base_url="http://graphite.local", **kwargs)


def _series(target: str, value, ts: int = 1700000000):
    return {"target": target, "datapoints": [[value, ts]]}


@pytest.fixture()
def backend():
    return _make_backend()


class TestGraphiteBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self, backend):
        data = [
            _series("pipewatch.pipeline_b.last_run", 1700000000),
            _series("pipewatch.pipeline_a.last_run", 1700000000),
        ]
        with patch.object(backend, "_query", return_value=data):
            result = backend.list_pipelines()
        assert result == ["pipeline_a", "pipeline_b"]

    def test_raises_on_non_200(self, backend):
        with patch.object(backend, "_query", side_effect=RuntimeError("404")):
            with pytest.raises(RuntimeError):
                backend.list_pipelines()

    def test_empty_when_no_series(self, backend):
        with patch.object(backend, "_query", return_value=[]):
            assert backend.list_pipelines() == []


class TestGraphiteBackendFetch:
    def test_parses_all_metrics(self, backend):
        ts = 1700000000

        def _query(target):
            if "last_run" in target:
                return [_series(target, float(ts))]
            if "error_count" in target:
                return [_series(target, 3.0)]
            if "row_count" in target:
                return [_series(target, 500.0)]
            return []

        with patch.object(backend, "_query", side_effect=_query):
            m = backend.fetch("my_pipeline")

        assert m.pipeline_id == "my_pipeline"
        assert m.last_run == datetime.fromtimestamp(ts, tz=timezone.utc)
        assert m.error_count == 3
        assert m.row_count == 500

    def test_returns_empty_metrics_when_no_data(self, backend):
        with patch.object(backend, "_query", return_value=[]):
            m = backend.fetch("missing")
        assert m.last_run is None
        assert m.error_count is None
        assert m.row_count is None

    def test_skips_null_datapoints(self, backend):
        data = [{"target": "pipewatch.p.last_run", "datapoints": [[None, 1700000000]]}]
        with patch.object(backend, "_query", return_value=data):
            m = backend.fetch("p")
        assert m.last_run is None


class TestGraphiteRegister:
    def test_backend_is_registered(self):
        import pipewatch.backends.graphite_register  # noqa: F401
        from pipewatch.backends import available_backends

        assert "graphite" in available_backends()
