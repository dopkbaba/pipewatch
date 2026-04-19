"""Tests for the NATS backend."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.nats import NATSBackend


def _make_message(data: Dict[str, Any]) -> MagicMock:
    msg = MagicMock()
    msg.data = json.dumps(data).encode()
    return msg


def _make_backend(messages: List[Dict[str, Any]]) -> NATSBackend:
    """Build a NATSBackend whose _connect is stubbed out, then fill cache."""
    with patch.object(NATSBackend, "_connect", return_value=None):
        backend = NATSBackend()
    for m in messages:
        pid = m.get("pipeline_id", "")
        if pid:
            backend._cache[pid] = backend._parse_metrics(m)
    return backend


class TestNATSBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend([
            {"pipeline_id": "pipe_b", "error_count": 0},
            {"pipeline_id": "pipe_a", "error_count": 1},
        ])
        assert backend.list_pipelines() == ["pipe_a", "pipe_b"]

    def test_empty_when_no_messages(self):
        backend = _make_backend([])
        assert backend.list_pipelines() == []


class TestNATSBackendFetch:
    def test_returns_empty_metrics_when_unknown(self):
        backend = _make_backend([])
        m = backend.fetch("missing")
        assert m.pipeline_id == "missing"
        assert m.last_run is None
        assert m.error_count is None

    def test_parses_aware_datetime(self):
        ts = "2024-03-15T10:00:00+00:00"
        backend = _make_backend([{"pipeline_id": "p1", "last_run": ts}])
        m = backend.fetch("p1")
        assert m.last_run is not None
        assert m.last_run.tzinfo is not None

    def test_makes_naive_datetime_aware(self):
        ts = "2024-03-15T10:00:00"
        backend = _make_backend([{"pipeline_id": "p1", "last_run": ts}])
        m = backend.fetch("p1")
        assert m.last_run.tzinfo == timezone.utc

    def test_parses_error_and_row_count(self):
        backend = _make_backend([{
            "pipeline_id": "p2",
            "error_count": 3,
            "row_count": 500,
        }])
        m = backend.fetch("p2")
        assert m.error_count == 3
        assert m.row_count == 500

    def test_missing_last_run_is_none(self):
        backend = _make_backend([{"pipeline_id": "p3"}])
        m = backend.fetch("p3")
        assert m.last_run is None
