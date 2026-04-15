"""Unit tests for MySQLBackend — all DB calls are mocked."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.mysql import MySQLBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(
    last_run: Optional[datetime] = None,
    error_rate: Optional[float] = None,
    row_count: Optional[int] = None,
) -> Dict[str, Any]:
    return {"last_run": last_run, "error_rate": error_rate, "row_count": row_count}


def _make_backend(rows: List[Dict[str, Any]], pipeline_ids: Optional[List[str]] = None):
    """Return a MySQLBackend whose connector is replaced with a mock."""
    backend = MySQLBackend()

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur

    # cursor used for fetch() returns dict rows
    mock_cur.fetchone.side_effect = rows
    # cursor used for list_pipelines() returns tuples
    mock_cur.fetchall.return_value = [(pid,) for pid in (pipeline_ids or [])]

    backend._conn = mock_conn
    return backend, mock_cur


# ---------------------------------------------------------------------------
# Tests: fetch
# ---------------------------------------------------------------------------

class TestMySQLBackendFetch:
    def test_returns_empty_metrics_when_no_row(self):
        backend, _ = _make_backend([None])
        result = backend.fetch("missing_pipe")
        assert result.pipeline_id == "missing_pipe"
        assert result.last_run is None
        assert result.error_rate is None
        assert result.row_count is None

    def test_parses_aware_datetime(self):
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        backend, _ = _make_backend([_make_row(last_run=ts, error_rate=0.01, row_count=500)])
        result = backend.fetch("pipe_a")
        assert result.last_run == ts
        assert result.last_run.tzinfo is not None

    def test_makes_naive_datetime_aware(self):
        naive_ts = datetime(2024, 3, 10, 8, 30, 0)  # no tzinfo
        backend, _ = _make_backend([_make_row(last_run=naive_ts)])
        result = backend.fetch("pipe_b")
        assert result.last_run is not None
        assert result.last_run.tzinfo == timezone.utc

    def test_parses_iso_string_timestamp(self):
        backend, _ = _make_backend([_make_row(last_run="2024-06-01T10:00:00")])
        result = backend.fetch("pipe_c")
        assert result.last_run == datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)

    def test_error_rate_and_row_count(self):
        backend, _ = _make_backend([_make_row(error_rate=0.05, row_count=1000)])
        result = backend.fetch("pipe_d")
        assert result.error_rate == pytest.approx(0.05)
        assert result.row_count == 1000


# ---------------------------------------------------------------------------
# Tests: list_pipelines
# ---------------------------------------------------------------------------

class TestMySQLBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend, _ = _make_backend(
            rows=[],
            pipeline_ids=["alpha", "beta", "gamma"],
        )
        result = backend.list_pipelines()
        assert result == ["alpha", "beta", "gamma"]

    def test_empty_when_no_rows(self):
        backend, _ = _make_backend(rows=[], pipeline_ids=[])
        assert backend.list_pipelines() == []


# ---------------------------------------------------------------------------
# Tests: lazy connect
# ---------------------------------------------------------------------------

def test_connect_uses_mysql_connector():
    backend = MySQLBackend(host="db", port=3306, user="u", password="p", database="pw")
    mock_module = MagicMock()
    mock_conn = MagicMock()
    mock_module.connect.return_value = mock_conn

    with patch.dict("sys.modules", {"mysql": mock_module, "mysql.connector": mock_module}):
        # Patch the import inside _connect
        with patch("pipewatch.backends.mysql.mysql.connector", mock_module, create=True):
            backend._conn = mock_conn  # pre-inject so we skip real import
            assert backend._connect() is mock_conn
