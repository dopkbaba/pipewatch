"""Tests for OpenSearchBackend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.opensearch import OpenSearchBackend
from pipewatch.backends.base import PipelineMetrics


def _make_backend() -> OpenSearchBackend:
    with patch("pipewatch.backends.opensearch.OpenSearchBackend._connect") as mock_conn:
        mock_conn.return_value = MagicMock()
        backend = OpenSearchBackend(host="localhost", port=9200, index="pipewatch")
    return backend


def _mock_client(backend: OpenSearchBackend) -> MagicMock:
    return backend._client  # type: ignore


class TestOpenSearchBackendFetch:
    def test_returns_empty_metrics_when_doc_missing(self):
        backend = _make_backend()
        _mock_client(backend).get.side_effect = Exception("not found")
        result = backend.fetch("pipe-1")
        assert isinstance(result, PipelineMetrics)
        assert result.pipeline_id == "pipe-1"
        assert result.last_run is None

    def test_returns_empty_metrics_when_source_empty(self):
        backend = _make_backend()
        _mock_client(backend).get.return_value = {"_source": {}}
        result = backend.fetch("pipe-1")
        assert result.last_run is None
        assert result.row_count is None

    def test_parses_aware_datetime(self):
        backend = _make_backend()
        _mock_client(backend).get.return_value = {
            "_source": {"last_run": "2024-03-01T10:00:00", "row_count": 500, "error_count": 2}
        }
        result = backend.fetch("pipe-1")
        assert result.last_run == datetime(2024, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
        assert result.row_count == 500
        assert result.error_count == 2

    def test_preserves_existing_tz(self):
        backend = _make_backend()
        _mock_client(backend).get.return_value = {
            "_source": {"last_run": "2024-03-01T10:00:00+00:00"}
        }
        result = backend.fetch("pipe-1")
        assert result.last_run.tzinfo is not None


class TestOpenSearchBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend()
        _mock_client(backend).search.return_value = {
            "hits": {"hits": [{"_id": "pipe-b"}, {"_id": "pipe-a"}]}
        }
        result = backend.list_pipelines()
        assert result == ["pipe-a", "pipe-b"]

    def test_returns_empty_when_no_hits(self):
        backend = _make_backend()
        _mock_client(backend).search.return_value = {"hits": {"hits": []}}
        assert backend.list_pipelines() == []


class TestOpenSearchRegister:
    def test_opensearch_backend_is_registered(self):
        from pipewatch.backends import available_backends
        import pipewatch.backends.opensearch_register  # noqa: F401
        assert "opensearch" in available_backends()

    def test_factory_creates_backend_with_defaults(self):
        with patch("pipewatch.backends.opensearch.OpenSearchBackend._connect"):
            from pipewatch.backends import get_backend
            backend = get_backend("opensearch", {})
        assert isinstance(backend, OpenSearchBackend)
        assert backend._host == "localhost"
        assert backend._index == "pipewatch"
