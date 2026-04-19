"""Tests for the Pub/Sub backend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.pubsub import PubSubBackend


def _make_message(attrs: dict) -> MagicMock:
    msg = MagicMock()
    msg.message.attributes = attrs
    msg.ack_id = f"ack-{attrs.get('pipeline_id', 'x')}"
    return msg


def _make_backend(messages: list) -> PubSubBackend:
    client = MagicMock()
    client.subscription_path.return_value = "projects/proj/subscriptions/pipewatch"
    response = MagicMock()
    response.received_messages = messages
    client.pull.return_value = response
    return PubSubBackend(project="proj", subscription="pipewatch", client=client)


class TestPubSubBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        msgs = [
            _make_message({"pipeline_id": "z_pipe", "last_run": "2024-01-02T00:00:00"}),
            _make_message({"pipeline_id": "a_pipe", "last_run": "2024-01-01T00:00:00"}),
        ]
        backend = _make_backend(msgs)
        assert backend.list_pipelines() == ["a_pipe", "z_pipe"]

    def test_empty_when_no_messages(self):
        backend = _make_backend([])
        assert backend.list_pipelines() == []

    def test_skips_messages_without_pipeline_id(self):
        msgs = [_make_message({"other_attr": "value"})]
        backend = _make_backend(msgs)
        assert backend.list_pipelines() == []


class TestPubSubBackendFetch:
    def test_returns_empty_metrics_when_not_seen(self):
        backend = _make_backend([])
        m = backend.fetch("missing")
        assert m.pipeline_id == "missing"
        assert m.last_run is None
        assert m.row_count is None

    def test_parses_aware_datetime(self):
        msgs = [_make_message({"pipeline_id": "p1", "last_run": "2024-03-15T12:00:00"})]
        backend = _make_backend(msgs)
        m = backend.fetch("p1")
        assert m.last_run is not None
        assert m.last_run.tzinfo == timezone.utc

    def test_parses_numeric_fields(self):
        msgs = [
            _make_message({
                "pipeline_id": "p2",
                "row_count": "500",
                "error_count": "3",
                "duration_seconds": "12.5",
            })
        ]
        backend = _make_backend(msgs)
        m = backend.fetch("p2")
        assert m.row_count == 500
        assert m.error_count == 3
        assert m.duration_seconds == pytest.approx(12.5)

    def test_acks_messages(self):
        client = MagicMock()
        client.subscription_path.return_value = "projects/proj/subscriptions/pw"
        response = MagicMock()
        response.received_messages = [
            _make_message({"pipeline_id": "p3", "last_run": "2024-01-01T00:00:00"})
        ]
        client.pull.return_value = response
        PubSubBackend(project="proj", subscription="pw", client=client)
        client.acknowledge.assert_called_once()
