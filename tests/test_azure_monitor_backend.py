"""Tests for AzureMonitorBackend."""
from __future__ import annotations

import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.azure_monitor import AzureMonitorBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_table(columns: List[str], rows: List[List[Any]]) -> Any:
    table = SimpleNamespace(columns=columns, rows=rows)
    return table


def _make_result(columns: List[str], rows: List[List[Any]]) -> Any:
    return SimpleNamespace(tables=[_make_table(columns, rows)])


def _make_backend(rows: Optional[List[List[Any]]] = None, columns: Optional[List[str]] = None):
    mock_client = MagicMock()
    result_cols = columns or ["pipeline_id", "last_run", "record_count", "error_count"]
    result_rows = rows if rows is not None else []
    mock_client.query_workspace.return_value = _make_result(result_cols, result_rows)

    with patch.object(AzureMonitorBackend, "_connect", return_value=mock_client):
        backend = AzureMonitorBackend(
            subscription_id="sub-123",
            resource_group="rg-test",
            workspace_id="ws-abc",
            credential=MagicMock(),
        )
    backend._client = mock_client
    return backend


# ---------------------------------------------------------------------------
# fetch() tests
# ---------------------------------------------------------------------------

class TestAzureMonitorBackendFetch:
    def test_returns_empty_metrics_when_no_row(self):
        backend = _make_backend(rows=[])
        metrics = backend.fetch("pipe-1")
        assert metrics.pipeline_id == "pipe-1"
        assert metrics.last_run is None
        assert metrics.record_count is None
        assert metrics.error_count is None

    def test_parses_aware_datetime(self):
        ts = "2024-03-15T10:00:00+00:00"
        backend = _make_backend(rows=[["pipe-1", ts, 500, 2]])
        metrics = backend.fetch("pipe-1")
        assert metrics.last_run is not None
        assert metrics.last_run.tzinfo is not None
        assert metrics.last_run.year == 2024

    def test_parses_naive_datetime_as_utc(self):
        ts = "2024-03-15T10:00:00"
        backend = _make_backend(rows=[["pipe-1", ts, 100, 0]])
        metrics = backend.fetch("pipe-1")
        assert metrics.last_run is not None
        assert metrics.last_run.tzinfo == datetime.timezone.utc

    def test_parses_record_and_error_counts(self):
        ts = "2024-03-15T10:00:00+00:00"
        backend = _make_backend(rows=[["pipe-2", ts, 999, 7]])
        metrics = backend.fetch("pipe-2")
        assert metrics.record_count == 999
        assert metrics.error_count == 7


# ---------------------------------------------------------------------------
# list_pipelines() tests
# ---------------------------------------------------------------------------

class TestAzureMonitorBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend(
            columns=["pipeline_id"],
            rows=[["pipe-b"], ["pipe-a"], ["pipe-c"]],
        )
        ids = backend.list_pipelines()
        assert ids == ["pipe-a", "pipe-b", "pipe-c"]

    def test_returns_empty_list_when_no_data(self):
        backend = _make_backend(columns=["pipeline_id"], rows=[])
        assert backend.list_pipelines() == []
