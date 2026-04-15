"""Tests for the PostgreSQL backend using a lightweight mock connection."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.postgres import PostgresBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(
    last_run: Optional[datetime] = None,
    row_count: Optional[int] = None,
    error_count: Optional[int] = None,
    duration_sec: Optional[float] = None,
) -> MagicMock:
    row = MagicMock()
    row.__getitem__ = lambda self, key: {
        "last_run": last_run,
        "row_count": row_count,
        "error_count": error_count,
        "duration_sec": duration_sec,
    }[key]
    return row


def _make_backend(rows_fetch=None, rows_list=None) -> PostgresBackend:
    """Return a PostgresBackend whose connection is fully mocked."""
    backend = PostgresBackend(dsn="postgresql://fake/db")

    mock_cursor = MagicMock()
    mock_cursor.__enter__ = lambda s: s
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = rows_fetch
    mock_cursor.fetchall.return_value = rows_list or []

    mock_conn = MagicMock()
    mock_conn.closed = False
    mock_conn.cursor.return_value = mock_cursor

    backend._conn = mock_conn
    return backend


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPostgresBackendFetch:
    def test_returns_empty_metrics_when_no_row(self):
        backend = _make_backend(rows_fetch=None)
        metrics = backend.fetch("pipe_a")
        assert metrics.pipeline_id == "pipe_a"
        assert metrics.last_run is None
        assert metrics.row_count is None

    def test_parses_aware_datetime(self):
        ts = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        row = _make_row(last_run=ts, row_count=100, error_count=0, duration_sec=1.5)
        backend = _make_backend(rows_fetch=row)
        metrics = backend.fetch("pipe_b")
        assert metrics.last_run == ts
        assert metrics.row_count == 100
        assert metrics.error_count == 0
        assert metrics.duration_sec == pytest.approx(1.5)

    def test_naive_datetime_becomes_utc(self):
        naive = datetime(2024, 3, 1, 8, 30, 0)
        row = _make_row(last_run=naive)
        backend = _make_backend(rows_fetch=row)
        metrics = backend.fetch("pipe_c")
        assert metrics.last_run is not None
        assert metrics.last_run.tzinfo == timezone.utc

    def test_pipeline_id_preserved(self):
        backend = _make_backend(rows_fetch=None)
        metrics = backend.fetch("my_pipeline")
        assert metrics.pipeline_id == "my_pipeline"


class TestPostgresBackendListPipelines:
    def test_returns_sorted_ids(self):
        backend = _make_backend(rows_list=[("zebra",), ("alpha",), ("middle",)])
        ids = backend.list_pipelines()
        assert ids == ["zebra", "alpha", "middle"]

    def test_empty_table(self):
        backend = _make_backend(rows_list=[])
        assert backend.list_pipelines() == []


class TestPostgresBackendRegistry:
    def test_factory_registers_backend(self):
        import pipewatch.backends.postgres_register  # noqa: F401 side-effect
        from pipewatch.backends import available_backends
        assert "postgres" in available_backends()

    def test_factory_creates_instance(self):
        from pipewatch.backends import get_backend
        with patch("pipewatch.backends.postgres.psycopg2.connect") as mock_connect:
            mock_connect.return_value = MagicMock(closed=False)
            backend = get_backend("postgres", {"dsn": "postgresql://fake/db"})
        assert isinstance(backend, PostgresBackend)
