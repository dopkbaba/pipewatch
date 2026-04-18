"""Tests for BigQueryBackend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.bigquery import BigQueryBackend


def _make_backend():
    backend = BigQueryBackend(project="proj", dataset="ds", table="pipeline_metrics")
    return backend


def _make_row(pipeline_id, last_run=None, row_count=None, error_rate=None):
    row = MagicMock()
    row.pipeline_id = pipeline_id
    row.last_run = last_run
    row.row_count = row_count
    row.error_rate = error_rate
    return row


class TestBigQueryBackendFetch:
    def test_returns_empty_metrics_when_no_row(self):
        backend = _make_backend()
        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = iter([])
        backend._client = mock_client

        result = backend.fetch("pipe_a")

        assert result.pipeline_id == "pipe_a"
        assert result.last_run is None
        assert result.row_count is None
        assert result.error_rate is None

    def test_parses_aware_datetime(self):
        backend = _make_backend()
        ts = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        row = _make_row("pipe_b", last_run=ts, row_count=500, error_rate=0.02)
        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = iter([row])
        backend._client = mock_client

        result = backend.fetch("pipe_b")

        assert result.last_run == ts
        assert result.last_run.tzinfo is not None

    def test_parses_naive_datetime_as_utc(self):
        backend = _make_backend()
        naive_ts = datetime(2024, 1, 15, 10, 0, 0)
        row = _make_row("pipe_c", last_run=naive_ts, row_count=100, error_rate=0.0)
        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = iter([row])
        backend._client = mock_client

        result = backend.fetch("pipe_c")

        assert result.last_run.tzinfo == timezone.utc

    def test_parses_row_count_and_error_rate(self):
        backend = _make_backend()
        ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
        row = _make_row("pipe_d", last_run=ts, row_count=9999, error_rate=0.05)
        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = iter([row])
        backend._client = mock_client

        result = backend.fetch("pipe_d")

        assert result.row_count == 9999
        assert result.error_rate == pytest.approx(0.05)


class TestBigQueryBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend()
        rows = [MagicMock(pipeline_id=pid) for pid in ["alpha", "beta", "gamma"]]
        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = iter(rows)
        backend._client = mock_client

        result = backend.list_pipelines()

        assert result == ["alpha", "beta", "gamma"]

    def test_returns_empty_list_when_no_pipelines(self):
        backend = _make_backend()
        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = iter([])
        backend._client = mock_client

        result = backend.list_pipelines()

        assert result == []
