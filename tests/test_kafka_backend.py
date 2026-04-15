"""Tests for pipewatch.backends.kafka.KafkaBackend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.kafka import KafkaBackend


REST_PROXY = "http://kafka-proxy:8082"
TOPIC = "pipewatch-metrics"


def _make_message(pipeline_id: str, **kwargs):
    value = {"pipeline_id": pipeline_id, **kwargs}
    return {"offset": 0, "value": value}


def _make_backend(messages):
    """Return a KafkaBackend whose HTTP call is stubbed with *messages*."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = messages
    with patch("pipewatch.backends.kafka.requests.get", return_value=mock_resp):
        return KafkaBackend(rest_proxy_url=REST_PROXY, topic=TOPIC)


# ---------------------------------------------------------------------------
# list_pipelines
# ---------------------------------------------------------------------------

class TestKafkaBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend([
            _make_message("zebra"),
            _make_message("alpha"),
            _make_message("mango"),
        ])
        assert backend.list_pipelines() == ["alpha", "mango", "zebra"]

    def test_empty_when_no_messages(self):
        backend = _make_backend([])
        assert backend.list_pipelines() == []

    def test_raises_on_non_200(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        with patch("pipewatch.backends.kafka.requests.get", return_value=mock_resp):
            with pytest.raises(RuntimeError, match="503"):
                KafkaBackend(rest_proxy_url=REST_PROXY, topic=TOPIC)


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------

class TestKafkaBackendFetch:
    def test_returns_empty_metrics_for_unknown_pipeline(self):
        backend = _make_backend([])
        m = backend.fetch("missing")
        assert m.pipeline_id == "missing"
        assert m.last_run is None
        assert m.row_count is None
        assert m.error_count is None

    def test_parses_last_run_as_aware_datetime(self):
        backend = _make_backend([
            _make_message("pipe1", last_run="2024-03-01T12:00:00+00:00")
        ])
        m = backend.fetch("pipe1")
        assert m.last_run == datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert m.last_run.tzinfo is not None

    def test_parses_naive_last_run_as_utc(self):
        backend = _make_backend([
            _make_message("pipe2", last_run="2024-03-01T08:30:00")
        ])
        m = backend.fetch("pipe2")
        assert m.last_run.tzinfo == timezone.utc

    def test_parses_row_and_error_counts(self):
        backend = _make_backend([
            _make_message("pipe3", row_count=500, error_count=3)
        ])
        m = backend.fetch("pipe3")
        assert m.row_count == 500
        assert m.error_count == 3

    def test_skips_messages_without_pipeline_id(self):
        backend = _make_backend([{"offset": 0, "value": {"row_count": 10}}])
        assert backend.list_pipelines() == []

    def test_latest_message_wins_for_same_pipeline(self):
        """When multiple messages share a pipeline_id the last one wins."""
        backend = _make_backend([
            _make_message("pipe4", row_count=100),
            _make_message("pipe4", row_count=999),
        ])
        assert backend.fetch("pipe4").row_count == 999
