"""Tests for the Elasticsearch backend."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.elasticsearch import ElasticsearchBackend


def _make_backend() -> ElasticsearchBackend:
    return ElasticsearchBackend(hosts=["http://localhost:9200"], index="pipewatch")


def _mock_client(backend: ElasticsearchBackend) -> MagicMock:
    client = MagicMock()
    backend._client = client
    return client


class TestElasticsearchBackendFetch:
    def test_returns_empty_metrics_when_doc_missing(self):
        backend = _make_backend()
        client = _mock_client(backend)
        client.get.side_effect = Exception("not found")
        m = backend.fetch("pipe-x")
        assert m.pipeline_id == "pipe-x"
        assert m.last_run is None
        assert m.error_count is None

    def test_parses_aware_datetime(self):
        backend = _make_backend()
        client = _mock_client(backend)
        client.get.return_value = {
            "_source": {
                "last_run": "2024-01-15T10:00:00",
                "error_count": 0,
                "row_count": 500,
                "duration_seconds": 12.5,
            }
        }
        m = backend.fetch("pipe-1")
        assert m.last_run is not None
        assert m.last_run.tzinfo == timezone.utc
        assert m.row_count == 500
        assert m.duration_seconds == 12.5

    def test_preserves_existing_tz(self):
        backend = _make_backend()
        client = _mock_client(backend)
        aware = datetime(2024, 3, 1, 8, 0, 0, tzinfo=timezone.utc)
        client.get.return_value = {"_source": {"last_run": aware}}
        m = backend.fetch("pipe-2")
        assert m.last_run == aware

    def test_parses_error_count(self):
        backend = _make_backend()
        client = _mock_client(backend)
        client.get.return_value = {"_source": {"error_count": 3}}
        m = backend.fetch("pipe-3")
        assert m.error_count == 3


class TestElasticsearchBackendListPipelines:
    def test_returns_sorted_ids(self):
        backend = _make_backend()
        client = _mock_client(backend)
        client.search.return_value = {
            "hits": {"hits": [{"_id": "z-pipe"}, {"_id": "a-pipe"}, {"_id": "m-pipe"}]}
        }
        result = backend.list_pipelines()
        assert result == ["a-pipe", "m-pipe", "z-pipe"]

    def test_returns_empty_list_when_no_docs(self):
        backend = _make_backend()
        client = _mock_client(backend)
        client.search.return_value = {"hits": {"hits": []}}
        assert backend.list_pipelines() == []
