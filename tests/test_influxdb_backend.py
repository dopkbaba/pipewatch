"""Tests for the InfluxDB backend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.influxdb import InfluxDBBackend


def _make_backend():
    with patch("pipewatch.backends.influxdb.InfluxDBBackend._connect", return_value=MagicMock()):
        b = InfluxDBBackend(url="http://fake:8086", token="tok", org="org", bucket="bkt")
    return b


def _make_record(field, value, pipeline_id="pipe1"):
    r = MagicMock()
    r.get_field.return_value = field
    r.get_value.return_value = value
    r.__getitem__ = lambda self, k: pipeline_id if k == "pipeline_id" else None
    return r


class TestInfluxDBBackendFetch:
    def test_returns_empty_metrics_when_no_data(self):
        b = _make_backend()
        b._client.query_api.return_value.query.return_value = []
        m = b.fetch("pipe1")
        assert m.pipeline_id == "pipe1"
        assert m.last_run is None
        assert m.records_processed is None
        assert m.error_count is None

    def test_parses_aware_datetime(self):
        b = _make_backend()
        table = MagicMock()
        rec_lr = _make_record("last_run", "2024-01-15T10:00:00Z")
        rec_rp = _make_record("records_processed", 500)
        rec_ec = _make_record("error_count", 2)
        table.records = [rec_lr, rec_rp, rec_ec]
        b._client.query_api.return_value.query.return_value = [table]
        m = b.fetch("pipe1")
        assert m.last_run == datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        assert m.records_processed == 500
        assert m.error_count == 2

    def test_parse_ts_returns_none_for_empty(self):
        assert InfluxDBBackend._parse_ts(None) is None
        assert InfluxDBBackend._parse_ts("") is None

    def test_parse_ts_adds_utc_when_naive(self):
        dt = InfluxDBBackend._parse_ts("2024-03-01T08:30:00")
        assert dt.tzinfo == timezone.utc


class TestInfluxDBBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        b = _make_backend()
        table = MagicMock()
        r1, r2 = MagicMock(), MagicMock()
        r1.__getitem__ = lambda self, k: "zeta" if k == "pipeline_id" else None
        r2.__getitem__ = lambda self, k: "alpha" if k == "pipeline_id" else None
        table.records = [r1, r2]
        b._client.query_api.return_value.query.return_value = [table]
        ids = b.list_pipelines()
        assert ids == ["alpha", "zeta"]

    def test_returns_empty_list_when_no_data(self):
        b = _make_backend()
        b._client.query_api.return_value.query.return_value = []
        assert b.list_pipelines() == []


class TestInfluxDBRegister:
    def test_influxdb_backend_is_registered(self):
        import pipewatch.backends.influxdb_register  # noqa: F401
        from pipewatch.backends import available_backends
        assert "influxdb" in available_backends()

    def test_factory_creates_backend_with_defaults(self):
        from pipewatch.backends.influxdb_register import _factory
        with patch("pipewatch.backends.influxdb.InfluxDBBackend._connect", return_value=MagicMock()):
            b = _factory({})
        assert b._bucket == "pipelines"
        assert b._org == "pipewatch"
        assert b._measurement == "pipeline_health"

    def test_factory_passes_custom_config(self):
        from pipewatch.backends.influxdb_register import _factory
        with patch("pipewatch.backends.influxdb.InfluxDBBackend._connect", return_value=MagicMock()):
            b = _factory({"bucket": "custom", "org": "myorg", "token": "secret"})
        assert b._bucket == "custom"
        assert b._org == "myorg"
        assert b._token == "secret"
