"""Tests for the SQS backend."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.backends.sqs import SQSBackend


def _make_message(body: dict) -> dict:
    return {"Body": json.dumps(body), "MessageId": "abc"}


def _make_backend(messages: list) -> SQSBackend:
    session = MagicMock()
    client = MagicMock()
    client.receive_message.return_value = {"Messages": messages}
    session.client.return_value = client
    return SQSBackend(
        queue_url="https://sqs.us-east-1.amazonaws.com/123/test",
        boto_session=session,
    )


class TestSQSBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend([
            _make_message({"pipeline_id": "pipe_b", "last_run": "2024-01-02T00:00:00"}),
            _make_message({"pipeline_id": "pipe_a", "last_run": "2024-01-01T00:00:00"}),
        ])
        assert backend.list_pipelines() == ["pipe_a", "pipe_b"]

    def test_empty_when_no_messages(self):
        backend = _make_backend([])
        assert backend.list_pipelines() == []

    def test_skips_messages_without_pipeline_id(self):
        backend = _make_backend([
            _make_message({"rows_processed": 5}),
            _make_message({"pipeline_id": "good"}),
        ])
        assert backend.list_pipelines() == ["good"]

    def test_skips_invalid_json(self):
        session = MagicMock()
        client = MagicMock()
        client.receive_message.return_value = {
            "Messages": [{"Body": "not-json", "MessageId": "x"}]
        }
        session.client.return_value = client
        backend = SQSBackend(queue_url="https://q", boto_session=session)
        assert backend.list_pipelines() == []


class TestSQSBackendFetch:
    def test_returns_empty_metrics_when_not_found(self):
        backend = _make_backend([])
        m = backend.fetch("missing")
        assert m.pipeline_id == "missing"
        assert m.last_run is None
        assert m.rows_processed is None

    def test_parses_aware_datetime(self):
        backend = _make_backend([
            _make_message({
                "pipeline_id": "p1",
                "last_run": "2024-03-15T12:00:00",
                "rows_processed": 100,
                "error_count": 2,
                "duration_seconds": 45.5,
            })
        ])
        m = backend.fetch("p1")
        assert m.last_run == datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert m.rows_processed == 100
        assert m.error_count == 2
        assert m.duration_seconds == 45.5

    def test_preserves_existing_timezone(self):
        from datetime import timedelta
        tz = timezone(timedelta(hours=5))
        ts = datetime(2024, 1, 1, 8, 0, 0, tzinfo=tz).isoformat()
        backend = _make_backend([_make_message({"pipeline_id": "p2", "last_run": ts})])
        m = backend.fetch("p2")
        assert m.last_run is not None
        assert m.last_run.tzinfo is not None
