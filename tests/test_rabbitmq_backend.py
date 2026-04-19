"""Tests for the RabbitMQ backend."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.rabbitmq import RabbitMQBackend


def _make_message(data: dict):
    body = json.dumps(data).encode()
    method = MagicMock()
    return method, MagicMock(), body


def _make_backend(messages=None):
    backend = RabbitMQBackend.__new__(RabbitMQBackend)
    backend._host = "localhost"
    backend._port = 5672
    backend._queue = "pipewatch"
    backend._username = "guest"
    backend._password = "guest"
    backend._max_messages = 200
    backend._store = {}
    backend._connected = True
    channel = MagicMock()
    if messages is None:
        messages = []
    # basic_get returns (None, None, None) when queue is empty
    returns = [_make_message(m) for m in messages] + [(None, None, None)]
    channel.basic_get.side_effect = returns
    backend._channel = channel
    backend._connection = MagicMock()
    return backend


class TestRabbitMQBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend([
            {"pipeline_id": "z_pipe", "row_count": 10},
            {"pipeline_id": "a_pipe", "row_count": 5},
        ])
        assert backend.list_pipelines() == ["a_pipe", "z_pipe"]

    def test_empty_when_no_messages(self):
        backend = _make_backend([])
        assert backend.list_pipelines() == []

    def test_skips_messages_without_pipeline_id(self):
        backend = _make_backend([{"row_count": 99}])
        assert backend.list_pipelines() == []

    def test_skips_invalid_json(self):
        backend = _make_backend()
        method = MagicMock()
        backend._channel.basic_get.side_effect = [
            (method, MagicMock(), b"not-json"),
            (None, None, None),
        ]
        assert backend.list_pipelines() == []


class TestRabbitMQBackendFetch:
    def test_returns_empty_metrics_when_no_message(self):
        backend = _make_backend([])
        m = backend.fetch("missing")
        assert m.pipeline_id == "missing"
        assert m.last_run is None
        assert m.row_count is None

    def test_parses_aware_datetime(self):
        backend = _make_backend([{
            "pipeline_id": "p1",
            "last_run": "2024-03-01T12:00:00+00:00",
            "row_count": 42,
            "error_rate": 0.01,
        }])
        m = backend.fetch("p1")
        assert m.last_run == datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert m.row_count == 42
        assert m.error_rate == pytest.approx(0.01)

    def test_parses_naive_datetime_as_utc(self):
        backend = _make_backend([{
            "pipeline_id": "p2",
            "last_run": "2024-03-01T08:00:00",
        }])
        m = backend.fetch("p2")
        assert m.last_run.tzinfo == timezone.utc
