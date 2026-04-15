"""Tests for the Prometheus backend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.prometheus import PrometheusBackend

SAMPLE_METRICS = """\
# HELP pipeline_last_run_timestamp Unix timestamp of last successful run
# TYPE pipeline_last_run_timestamp gauge
pipeline_last_run_timestamp{pipeline="orders"} 1700000000
pipeline_last_run_timestamp{pipeline="users"} 1700001000
# HELP pipeline_row_count Rows processed in last run
# TYPE pipeline_row_count gauge
pipeline_row_count{pipeline="orders"} 4200
pipeline_row_count{pipeline="users"} 980
# HELP pipeline_error_count Errors in last run
# TYPE pipeline_error_count gauge
pipeline_error_count{pipeline="orders"} 0
pipeline_error_count{pipeline="users"} 3
"""


@pytest.fixture()
def backend():
    return PrometheusBackend(base_url="http://prometheus.example.com")


def _mock_response(text: str, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    return resp


class TestPrometheusBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self, backend):
        with patch("requests.get", return_value=_mock_response(SAMPLE_METRICS)):
            ids = backend.list_pipelines()
        assert ids == ["orders", "users"]

    def test_raises_on_non_200(self, backend):
        with patch("requests.get", return_value=_mock_response("", status=503)):
            with pytest.raises(ConnectionError, match="503"):
                backend.list_pipelines()


class TestPrometheusBackendFetch:
    def test_returns_metrics_for_known_pipeline(self, backend):
        with patch("requests.get", return_value=_mock_response(SAMPLE_METRICS)):
            m = backend.fetch("orders")
        assert m is not None
        assert m.pipeline_id == "orders"
        assert m.row_count == 4200
        assert m.error_count == 0

    def test_last_run_is_utc_aware(self, backend):
        with patch("requests.get", return_value=_mock_response(SAMPLE_METRICS)):
            m = backend.fetch("orders")
        assert m.last_run is not None
        assert m.last_run.tzinfo == timezone.utc
        assert m.last_run == datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)

    def test_returns_none_for_unknown_pipeline(self, backend):
        with patch("requests.get", return_value=_mock_response(SAMPLE_METRICS)):
            m = backend.fetch("nonexistent")
        assert m is None

    def test_error_count_for_users(self, backend):
        with patch("requests.get", return_value=_mock_response(SAMPLE_METRICS)):
            m = backend.fetch("users")
        assert m is not None
        assert m.error_count == 3
        assert m.row_count == 980

    def test_missing_optional_metrics_are_none(self, backend):
        sparse = 'pipeline_last_run_timestamp{pipeline="sparse"} 1700000000\n'
        with patch("requests.get", return_value=_mock_response(sparse)):
            m = backend.fetch("sparse")
        assert m is not None
        assert m.row_count is None
        assert m.error_count is None
