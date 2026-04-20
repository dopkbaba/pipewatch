"""Tests for the MQTT backend."""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.mqtt import MQTTBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_backend() -> MQTTBackend:
    """Return a MQTTBackend with the paho client fully mocked out."""
    with patch("pipewatch.backends.mqtt.MQTTBackend._connect", return_value=MagicMock()):
        return MQTTBackend(host="broker", port=1883, topic="pipewatch/#")


def _make_message(payload: Dict[str, Any]):
    msg = SimpleNamespace(payload=json.dumps(payload).encode())
    return msg


# ---------------------------------------------------------------------------
# list_pipelines
# ---------------------------------------------------------------------------

class TestMQTTBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend()
        backend._on_message(None, None, _make_message({"pipeline_id": "beta", "row_count": 5}))
        backend._on_message(None, None, _make_message({"pipeline_id": "alpha", "row_count": 3}))
        assert backend.list_pipelines() == ["alpha", "beta"]

    def test_empty_when_no_messages(self):
        backend = _make_backend()
        assert backend.list_pipelines() == []

    def test_ignores_messages_without_pipeline_id(self):
        backend = _make_backend()
        backend._on_message(None, None, _make_message({"row_count": 10}))
        assert backend.list_pipelines() == []

    def test_ignores_invalid_json(self):
        backend = _make_backend()
        bad = SimpleNamespace(payload=b"not-json")
        backend._on_message(None, None, bad)
        assert backend.list_pipelines() == []


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------

class TestMQTTBackendFetch:
    def test_returns_empty_metrics_when_unknown(self):
        backend = _make_backend()
        m = backend.fetch("missing")
        assert m.pipeline_id == "missing"
        assert m.last_run is None
        assert m.row_count is None
        assert m.error_count is None

    def test_parses_aware_datetime(self):
        backend = _make_backend()
        ts = "2024-03-01T12:00:00+00:00"
        backend._on_message(None, None, _make_message(
            {"pipeline_id": "p1", "last_run": ts, "row_count": 100, "error_count": 0}
        ))
        m = backend.fetch("p1")
        assert m.last_run == datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_parses_naive_datetime_as_utc(self):
        backend = _make_backend()
        backend._on_message(None, None, _make_message(
            {"pipeline_id": "p2", "last_run": "2024-01-15T08:30:00", "row_count": 50}
        ))
        m = backend.fetch("p2")
        assert m.last_run is not None
        assert m.last_run.tzinfo == timezone.utc

    def test_row_and_error_count_populated(self):
        backend = _make_backend()
        backend._on_message(None, None, _make_message(
            {"pipeline_id": "p3", "row_count": 42, "error_count": 7}
        ))
        m = backend.fetch("p3")
        assert m.row_count == 42
        assert m.error_count == 7

    def test_latest_message_overwrites_previous(self):
        backend = _make_backend()
        backend._on_message(None, None, _make_message({"pipeline_id": "p4", "row_count": 1}))
        backend._on_message(None, None, _make_message({"pipeline_id": "p4", "row_count": 99}))
        assert backend.fetch("p4").row_count == 99
