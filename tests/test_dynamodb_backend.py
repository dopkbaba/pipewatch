"""Tests for DynamoDBBackend."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.dynamodb import DynamoDBBackend


def _make_backend():
    backend = DynamoDBBackend.__new__(DynamoDBBackend)
    backend.table_name = "pipelines"
    backend.region_name = "us-east-1"
    backend.endpoint_url = None
    backend._table = MagicMock()
    return backend


def _make_table(item=None):
    table = MagicMock()
    table.get_item.return_value = {"Item": item} if item else {}
    return table


class TestDynamoDBBackendFetch:
    def test_returns_empty_metrics_when_no_item(self):
        backend = _make_backend()
        backend._table = _make_table()
        m = backend.fetch("pipe-1")
        assert m.pipeline_id == "pipe-1"
        assert m.last_run is None
        assert m.error_rate is None
        assert m.row_count is None

    def test_parses_aware_datetime(self):
        backend = _make_backend()
        backend._table = _make_table({"pipeline_id": "p", "last_run": "2024-01-15T10:00:00+00:00"})
        m = backend.fetch("p")
        assert m.last_run == datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    def test_assumes_utc_for_naive_datetime(self):
        backend = _make_backend()
        backend._table = _make_table({"pipeline_id": "p", "last_run": "2024-01-15T10:00:00"})
        m = backend.fetch("p")
        assert m.last_run.tzinfo == timezone.utc

    def test_parses_error_rate_and_row_count(self):
        backend = _make_backend()
        backend._table = _make_table({
            "pipeline_id": "p",
            "error_rate": Decimal("0.05"),
            "row_count": Decimal("500"),
        })
        m = backend.fetch("p")
        assert m.error_rate == pytest.approx(0.05)
        assert m.row_count == 500


class TestDynamoDBBackendListPipelines:
    def test_returns_sorted_ids(self):
        backend = _make_backend()
        backend._table.scan.return_value = {
            "Items": [{"pipeline_id": "z"}, {"pipeline_id": "a"}, {"pipeline_id": "m"}]
        }
        assert backend.list_pipelines() == ["a", "m", "z"]

    def test_returns_empty_list_when_no_items(self):
        backend = _make_backend()
        backend._table.scan.return_value = {"Items": []}
        assert backend.list_pipelines() == []
