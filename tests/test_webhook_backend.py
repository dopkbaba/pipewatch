"""Tests for WebhookBackend."""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.webhook import WebhookBackend


BASE_URL = "https://hooks.example.com"


def _make_backend(**kwargs) -> WebhookBackend:
    return WebhookBackend(base_url=BASE_URL, **kwargs)


def _mock_response(json_data, status_code: int = 200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    return resp


# ---------------------------------------------------------------------------
# list_pipelines
# ---------------------------------------------------------------------------

class TestWebhookBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        data = [
            {"pipeline_id": "beta"},
            {"pipeline_id": "alpha"},
        ]
        with patch("requests.get", return_value=_mock_response(data)):
            result = _make_backend().list_pipelines()
        assert result == ["alpha", "beta"]

    def test_raises_on_non_200(self):
        with patch("requests.get", return_value=_mock_response({}, status_code=503)):
            with pytest.raises(RuntimeError, match="HTTP 503"):
                _make_backend().list_pipelines()

    def test_skips_items_without_pipeline_id(self):
        data = [{"pipeline_id": "ok"}, {"other": "junk"}]
        with patch("requests.get", return_value=_mock_response(data)):
            result = _make_backend().list_pipelines()
        assert result == ["ok"]


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------

class TestWebhookBackendFetch:
    def test_returns_empty_metrics_when_fields_missing(self):
        data = {"pipeline_id": "p1"}
        with patch("requests.get", return_value=_mock_response(data)):
            m = _make_backend().fetch("p1")
        assert m.pipeline_id == "p1"
        assert m.last_run is None
        assert m.error_rate is None
        assert m.row_count is None

    def test_parses_aware_datetime(self):
        data = {"pipeline_id": "p1", "last_run": "2024-03-10T08:00:00Z"}
        with patch("requests.get", return_value=_mock_response(data)):
            m = _make_backend().fetch("p1")
        assert m.last_run == datetime.datetime(2024, 3, 10, 8, 0, tzinfo=datetime.timezone.utc)

    def test_parses_full_metrics(self):
        data = {"pipeline_id": "p2", "error_rate": 0.05, "row_count": 9999,
                "last_run": "2024-03-10T12:00:00+00:00"}
        with patch("requests.get", return_value=_mock_response(data)):
            m = _make_backend().fetch("p2")
        assert m.error_rate == pytest.approx(0.05)
        assert m.row_count == 9999

    def test_sends_auth_header_when_token_provided(self):
        data = {"pipeline_id": "p1"}
        with patch("requests.get", return_value=_mock_response(data)) as mock_get:
            _make_backend(token="secret").fetch("p1")
        _, kwargs = mock_get.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer secret"

    def test_raises_on_non_200(self):
        with patch("requests.get", return_value=_mock_response({}, status_code=404)):
            with pytest.raises(RuntimeError, match="HTTP 404"):
                _make_backend().fetch("missing")
