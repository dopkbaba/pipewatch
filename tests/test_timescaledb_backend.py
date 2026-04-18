"""Tests for TimescaleDBBackend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.timescaledb import TimescaleDBBackend


def _make_backend() -> TimescaleDBBackend:
    backend = TimescaleDBBackend.__new__(TimescaleDBBackend)
    backend._dsn = "host=localhost"
    backend._table = "pipeline_metrics"
    backend._conn = None
    return backend


def _make_conn(rows_map: dict):
    """rows_map: {pipeline_id: row_tuple | None}"""
    conn = MagicMock()

    def cursor_ctx():
        cur = MagicMock()
        cur.__enter__ = lambda s: s
        cur.__exit__ = MagicMock(return_value=False)
        return cur

    conn.cursor.side_effect = cursor_ctx
    return conn, rows_map


class TestTimescaleDBBackendFetch:
    def _backend_with_row(self, row):
        b = _make_backend()
        cur = MagicMock()
        cur.__enter__ = lambda s: s
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchone.return_value = row
        conn = MagicMock()
        conn.cursor.return_value = cur
        b._conn = conn
        return b

    def test_returns_empty_metrics_when_no_row(self):
        b = self._backend_with_row(None)
        m = b.fetch("pipe1")
        assert m.pipeline_id == "pipe1"
        assert m.last_run is None
        assert m.record_count is None
        assert m.error_count is None

    def test_parses_aware_datetime(self):
        ts = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        b = self._backend_with_row((ts, 500, 2))
        m = b.fetch("pipe1")
        assert m.last_run == ts
        assert m.last_run.tzinfo is not None

    def test_makes_naive_datetime_aware(self):
        ts = datetime(2024, 1, 15, 10, 0, 0)  # naive
        b = self._backend_with_row((ts, 100, 0))
        m = b.fetch("pipe1")
        assert m.last_run.tzinfo == timezone.utc

    def test_record_and_error_counts(self):
        ts = datetime(2024, 6, 1, tzinfo=timezone.utc)
        b = self._backend_with_row((ts, 9999, 7))
        m = b.fetch("pipe_x")
        assert m.record_count == 9999
        assert m.error_count == 7


class TestTimescaleDBBackendListPipelines:
    def _backend_with_ids(self, ids):
        b = _make_backend()
        cur = MagicMock()
        cur.__enter__ = lambda s: s
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchall.return_value = [(i,) for i in ids]
        conn = MagicMock()
        conn.cursor.return_value = cur
        b._conn = conn
        return b

    def test_returns_sorted_pipeline_ids(self):
        b = self._backend_with_ids(["alpha", "beta", "gamma"])
        assert b.list_pipelines() == ["alpha", "beta", "gamma"]

    def test_returns_empty_list_when_no_pipelines(self):
        b = self._backend_with_ids([])
        assert b.list_pipelines() == []
