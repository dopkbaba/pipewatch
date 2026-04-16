"""Tests for the MongoDB backend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.mongodb import MongoDBBackend


def _make_backend():
    backend = MongoDBBackend.__new__(MongoDBBackend)
    backend._uri = "mongodb://localhost:27017"
    backend._database = "pipewatch"
    backend._collection = "pipeline_metrics"
    backend._client = None
    return backend


def _make_col(doc):
    col = MagicMock()
    col.find_one.return_value = doc
    col.distinct.return_value = ["pipe_b", "pipe_a"]
    return col


class TestMongoDBBackendFetch:
    def test_returns_empty_metrics_when_no_doc(self):
        backend = _make_backend()
        col = _make_col(None)
        with patch.object(backend, "_connect", return_value=col):
            result = backend.fetch("missing")
        assert result.pipeline_id == "missing"
        assert result.last_run is None
        assert result.rows_processed is None

    def test_parses_aware_datetime(self):
        backend = _make_backend()
        ts = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        col = _make_col({"pipeline_id": "p1", "last_run": ts, "rows_processed": 500})
        with patch.object(backend, "_connect", return_value=col):
            result = backend.fetch("p1")
        assert result.last_run == ts
        assert result.last_run.tzinfo is not None

    def test_makes_naive_datetime_aware(self):
        backend = _make_backend()
        ts = datetime(2024, 1, 15, 10, 0, 0)
        col = _make_col({"pipeline_id": "p1", "last_run": ts})
        with patch.object(backend, "_connect", return_value=col):
            result = backend.fetch("p1")
        assert result.last_run.tzinfo == timezone.utc

    def test_parses_numeric_fields(self):
        backend = _make_backend()
        col = _make_col({
            "pipeline_id": "p1",
            "rows_processed": 1000,
            "error_count": 3,
            "duration_seconds": 42.5,
        })
        with patch.object(backend, "_connect", return_value=col):
            result = backend.fetch("p1")
        assert result.rows_processed == 1000
        assert result.error_count == 3
        assert result.duration_seconds == 42.5


class TestMongoDBBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend()
        col = _make_col(None)
        with patch.object(backend, "_connect", return_value=col):
            ids = backend.list_pipelines()
        assert ids == ["pipe_a", "pipe_b"]

    def test_empty_when_no_documents(self):
        backend = _make_backend()
        col = MagicMock()
        col.distinct.return_value = []
        with patch.object(backend, "_connect", return_value=col):
            ids = backend.list_pipelines()
        assert ids == []
