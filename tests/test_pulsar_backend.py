"""Tests for PulsarBackend."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.pulsar import PulsarBackend


def _make_message(data: dict):
    msg = MagicMock()
    msg.data.return_value = json.dumps(data).encode()
    return msg


def _make_backend(messages: list) -> PulsarBackend:
    """Build a PulsarBackend with a fake Pulsar client."""
    call_count = 0

    def fake_receive(timeout_millis=3000):
        nonlocal call_count
        if call_count < len(messages):
            m = messages[call_count]
            call_count += 1
            return m
        raise Exception("timeout")

    fake_consumer = MagicMock()
    fake_consumer.receive.side_effect = fake_receive

    fake_client = MagicMock()
    fake_client.subscribe.return_value = fake_consumer

    fake_pulsar = MagicMock()
    fake_pulsar.Client.return_value = fake_client
    fake_pulsar.ConsumerType.Shared = "Shared"

    with patch.dict("sys.modules", {"pulsar": fake_pulsar}):
        backend = PulsarBackend.__new__(PulsarBackend)
        backend._service_url = "pulsar://localhost:6650"
        backend._topic = "pipewatch-metrics"
        backend._subscription = "pipewatch-sub"
        backend._receive_timeout_ms = 3000
        backend._store = {}
        backend._consumer = fake_consumer
        backend._refresh()
    return backend


class TestPulsarBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        msgs = [
            _make_message({"pipeline_id": "z-pipe"}),
            _make_message({"pipeline_id": "a-pipe"}),
        ]
        backend = _make_backend(msgs)
        assert backend.list_pipelines() == ["a-pipe", "z-pipe"]

    def test_empty_when_no_messages(self):
        backend = _make_backend([])
        assert backend.list_pipelines() == []


class TestPulsarBackendFetch:
    def test_returns_empty_metrics_when_unknown(self):
        backend = _make_backend([])
        m = backend.fetch("missing")
        assert m.pipeline_id == "missing"
        assert m.last_run is None
        assert m.record_count is None

    def test_parses_aware_datetime(self):
        ts = "2024-03-01T12:00:00+00:00"
        msgs = [_make_message({"pipeline_id": "p1", "last_run": ts, "record_count": 50})]
        backend = _make_backend(msgs)
        m = backend.fetch("p1")
        assert m.last_run == datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert m.record_count == 50

    def test_naive_datetime_becomes_utc(self):
        msgs = [_make_message({"pipeline_id": "p2", "last_run": "2024-01-15T08:30:00"})]
        backend = _make_backend(msgs)
        m = backend.fetch("p2")
        assert m.last_run is not None
        assert m.last_run.tzinfo == timezone.utc

    def test_error_count_parsed(self):
        msgs = [_make_message({"pipeline_id": "p3", "error_count": 7})]
        backend = _make_backend(msgs)
        assert backend.fetch("p3").error_count == 7
