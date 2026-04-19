"""Tests for KinesisBackend."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from pipewatch.backends.kinesis import KinesisBackend


def _make_record(data: Dict[str, Any]) -> Dict[str, Any]:
    return {"Data": json.dumps(data).encode()}


def _make_backend(records: List[Dict[str, Any]]) -> KinesisBackend:
    backend = KinesisBackend.__new__(KinesisBackend)
    backend._stream_name = "test-stream"
    backend._region = "us-east-1"
    backend._iterator_type = "LATEST"
    backend._boto_kwargs = {}
    backend._cache = {}

    client = MagicMock()
    client.list_shards.return_value = {"Shards": [{"ShardId": "shardId-000"}]}
    client.get_shard_iterator.return_value = {"ShardIterator": "iter-abc"}
    client.get_records.return_value = {"Records": [_make_record(r) for r in records]}
    backend._client = client
    return backend


class TestKinesisBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend([
            {"pipeline_id": "z_pipe"},
            {"pipeline_id": "a_pipe"},
        ])
        assert backend.list_pipelines() == ["a_pipe", "z_pipe"]

    def test_empty_when_no_records(self):
        backend = _make_backend([])
        assert backend.list_pipelines() == []

    def test_skips_records_without_pipeline_id(self):
        backend = _make_backend([{"record_count": 5}])
        assert backend.list_pipelines() == []

    def test_skips_invalid_json(self):
        backend = KinesisBackend.__new__(KinesisBackend)
        backend._stream_name = "s"
        backend._region = "us-east-1"
        backend._iterator_type = "LATEST"
        backend._boto_kwargs = {}
        backend._cache = {}
        client = MagicMock()
        client.list_shards.return_value = {"Shards": [{"ShardId": "shardId-000"}]}
        client.get_shard_iterator.return_value = {"ShardIterator": "iter"}
        client.get_records.return_value = {"Records": [{"Data": b"not-json"}]}
        backend._client = client
        assert backend.list_pipelines() == []


class TestKinesisBackendFetch:
    def test_returns_empty_metrics_when_no_record(self):
        backend = _make_backend([])
        m = backend.fetch("missing")
        assert m.pipeline_id == "missing"
        assert m.last_run is None
        assert m.record_count is None

    def test_parses_aware_datetime(self):
        backend = _make_backend([{
            "pipeline_id": "p1",
            "last_run": "2024-01-15T10:00:00+00:00",
            "record_count": 42,
            "error_count": 0,
            "duration_seconds": 5.5,
        }])
        m = backend.fetch("p1")
        assert m.last_run == datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        assert m.record_count == 42
        assert m.error_count == 0
        assert m.duration_seconds == 5.5

    def test_parses_naive_datetime_as_utc(self):
        backend = _make_backend([{
            "pipeline_id": "p2",
            "last_run": "2024-03-01T08:30:00",
        }])
        m = backend.fetch("p2")
        assert m.last_run is not None
        assert m.last_run.tzinfo == timezone.utc
