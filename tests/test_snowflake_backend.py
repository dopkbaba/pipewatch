"""Tests for the Snowflake backend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.snowflake import SnowflakeBackend


def _make_backend() -> SnowflakeBackend:
    return SnowflakeBackend(
        account="acct",
        user="user",
        password="pass",
        database="db",
    )


def _make_conn(rows_metrics=None, rows_list=None):
    cur = MagicMock()
    cur.fetchone.return_value = rows_metrics
    cur.fetchall.return_value = rows_list or []
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


class TestSnowflakeBackendFetch:
    def test_returns_empty_metrics_when_no_row(self):
        backend = _make_backend()
        backend._conn = _make_conn(rows_metrics=None)
        result = backend.fetch("pipe_a")
        assert result.pipeline_id == "pipe_a"
        assert result.last_run is None
        assert result.row_count is None
        assert result.error_rate is None

    def test_parses_aware_datetime(self):
        ts = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        backend = _make_backend()
        backend._conn = _make_conn(rows_metrics=(ts, 500, 0.01))
        result = backend.fetch("pipe_b")
        assert result.last_run == ts
        assert result.last_run.tzinfo is not None

    def test_makes_naive_datetime_aware(self):
        ts = datetime(2024, 1, 15, 10, 0, 0)  # naive
        backend = _make_backend()
        backend._conn = _make_conn(rows_metrics=(ts, 100, 0.0))
        result = backend.fetch("pipe_c")
        assert result.last_run.tzinfo == timezone.utc

    def test_parses_row_count_and_error_rate(self):
        ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
        backend = _make_backend()
        backend._conn = _make_conn(rows_metrics=(ts, 9999, 0.05))
        result = backend.fetch("pipe_d")
        assert result.row_count == 9999
        assert result.error_rate == pytest.approx(0.05)

    def test_handles_none_error_rate(self):
        ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
        backend = _make_backend()
        backend._conn = _make_conn(rows_metrics=(ts, 42, None))
        result = backend.fetch("pipe_e")
        assert result.error_rate is None


class TestSnowflakeBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend()
        backend._conn = _make_conn(rows_list=[("alpha",), ("beta",), ("gamma",)])
        ids = backend.list_pipelines()
        assert ids == ["alpha", "beta", "gamma"]

    def test_returns_empty_list_when_no_pipelines(self):
        backend = _make_backend()
        backend._conn = _make_conn(rows_list=[])
        assert backend.list_pipelines() == []
