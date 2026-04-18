"""Tests for the gRPC backend."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Dict, List, Optional

import pytest

from pipewatch.backends.grpc import GrpcBackend


# ---------------------------------------------------------------------------
# Fake stub helpers
# ---------------------------------------------------------------------------

class _FakeStub:
    """Minimal stand-in for a generated gRPC stub."""

    def __init__(self, pipeline_ids: List[str], metrics: Dict[str, dict]):
        self._ids = pipeline_ids
        self._metrics = metrics

    def ListPipelines(self, _request, timeout=None):
        return SimpleNamespace(pipeline_ids=self._ids)

    def GetMetrics(self, request, timeout=None):
        pid = request["pipeline_id"]
        m = self._metrics.get(pid)
        if m is None:
            return SimpleNamespace(pipeline_id="")
        return SimpleNamespace(pipeline_id=pid, **m)


def _make_backend(ids=None, metrics=None) -> GrpcBackend:
    return GrpcBackend(
        stub=_FakeStub(ids or [], metrics or {}),
        timeout=1.0,
    )


# ---------------------------------------------------------------------------
# list_pipelines
# ---------------------------------------------------------------------------

class TestGrpcBackendListPipelines:
    def test_returns_sorted_ids(self):
        b = _make_backend(ids=["z_pipe", "a_pipe", "m_pipe"])
        assert b.list_pipelines() == ["a_pipe", "m_pipe", "z_pipe"]

    def test_empty_when_no_pipelines(self):
        b = _make_backend(ids=[])
        assert b.list_pipelines() == []


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------

class TestGrpcBackendFetch:
    def test_returns_empty_metrics_when_no_row(self):
        b = _make_backend()
        m = b.fetch("missing")
        assert m.pipeline_id == "missing"
        assert m.last_run is None
        assert m.row_count is None

    def test_parses_aware_datetime(self):
        b = _make_backend(
            ids=["p1"],
            metrics={"p1": {"last_run": "2024-03-01T12:00:00+00:00",
                            "row_count": 500, "error_count": 0,
                            "duration_seconds": 42.0}},
        )
        m = b.fetch("p1")
        assert m.last_run == datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert m.row_count == 500
        assert m.duration_seconds == 42.0

    def test_parses_naive_datetime_as_utc(self):
        b = _make_backend(
            ids=["p2"],
            metrics={"p2": {"last_run": "2024-06-15T08:30:00",
                            "row_count": 0, "error_count": 1,
                            "duration_seconds": 0.0}},
        )
        m = b.fetch("p2")
        assert m.last_run.tzinfo == timezone.utc

    def test_error_count_propagated(self):
        b = _make_backend(
            ids=["p3"],
            metrics={"p3": {"last_run": "2024-01-01T00:00:00Z",
                            "row_count": 10, "error_count": 3,
                            "duration_seconds": 5.5}},
        )
        assert b.fetch("p3").error_count == 3
