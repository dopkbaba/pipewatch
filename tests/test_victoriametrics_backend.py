"""Tests for the VictoriaMetrics backend."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.victoriametrics import VictoriaMetricsBackend


def _make_backend(base_url: str = "http://vm:8428") -> VictoriaMetricsBackend:
    return VictoriaMetricsBackend(base_url=base_url, timeout=5)


def _mock_response(payload: dict, status: int = 200):
    resp = MagicMock()
    resp.status = status
    resp.read.return_value = json.dumps(payload).encode()
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def _vm_result(pipeline_id: str, value: str):
    return {"metric": {"pipeline_id": pipeline_id}, "value": [0, value]}


class TestVictoriaMetricsBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend()
        payload = {
            "data": {
                "result": [
                    _vm_result("pipe_b", "1700000000"),
                    _vm_result("pipe_a", "1700000001"),
                ]
            }
        }
        with patch("urllib.request.urlopen", return_value=_mock_response(payload)):
            ids = backend.list_pipelines()
        assert ids == ["pipe_a", "pipe_b"]

    def test_returns_empty_when_no_results(self):
        backend = _make_backend()
        payload = {"data": {"result": []}}
        with patch("urllib.request.urlopen", return_value=_mock_response(payload)):
            ids = backend.list_pipelines()
        assert ids == []

    def test_raises_on_non_200(self):
        backend = _make_backend()
        with patch(
            "urllib.request.urlopen",
            return_value=_mock_response({}, status=503),
        ):
            with pytest.raises(RuntimeError, match="HTTP 503"):
                backend.list_pipelines()


class TestVictoriaMetricsBackendFetch:
    def test_parses_last_run_timestamp(self):
        backend = _make_backend()
        ts = 1_700_000_000
        ts_payload = {"data": {"result": [_vm_result("pipe_a", str(ts))]}}
        empty = {"data": {"result": []}}

        responses = [
            _mock_response(ts_payload),
            _mock_response(empty),
            _mock_response(empty),
        ]
        with patch("urllib.request.urlopen", side_effect=responses):
            metrics = backend.fetch("pipe_a")

        expected = datetime.fromtimestamp(ts, tz=timezone.utc)
        assert metrics.last_run == expected
        assert metrics.pipeline_id == "pipe_a"

    def test_parses_error_and_row_counts(self):
        backend = _make_backend()
        empty_ts = {"data": {"result": []}}
        err_payload = {"data": {"result": [_vm_result("pipe_a", "3")]}}
        row_payload = {"data": {"result": [_vm_result("pipe_a", "1000")]}}

        responses = [
            _mock_response(empty_ts),
            _mock_response(err_payload),
            _mock_response(row_payload),
        ]
        with patch("urllib.request.urlopen", side_effect=responses):
            metrics = backend.fetch("pipe_a")

        assert metrics.error_count == 3
        assert metrics.row_count == 1000
        assert metrics.last_run is None

    def test_returns_empty_metrics_when_no_data(self):
        backend = _make_backend()
        empty = {"data": {"result": []}}
        with patch(
            "urllib.request.urlopen",
            side_effect=[_mock_response(empty)] * 3,
        ):
            metrics = backend.fetch("missing")

        assert metrics.pipeline_id == "missing"
        assert metrics.last_run is None
        assert metrics.error_count is None
        assert metrics.row_count is None


class TestVictoriaMetricsRegister:
    def test_factory_creates_backend_with_defaults(self):
        from pipewatch.backends.victoriametrics_register import _factory

        backend = _factory({})
        assert isinstance(backend, VictoriaMetricsBackend)
        assert backend._base_url == "http://localhost:8428"
        assert backend._timeout == 10

    def test_factory_passes_custom_config(self):
        from pipewatch.backends.victoriametrics_register import _factory

        backend = _factory({"base_url": "http://vm-prod:8428", "timeout": "30"})
        assert backend._base_url == "http://vm-prod:8428"
        assert backend._timeout == 30
