"""Tests for the Datadog backend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.datadog import DatadogBackend


def _make_backend() -> DatadogBackend:
    return DatadogBackend(api_key="fake-api", app_key="fake-app")


def _series_response(pipeline_id: str, value: float):
    return {
        "series": [
            {
                "tags": [f"pipeline_id:{pipeline_id}"],
                "pointlist": [[1_700_000_000_000, value]],
            }
        ]
    }


class TestDatadogBackendFetch:
    def test_returns_empty_metrics_when_no_series(self):
        backend = _make_backend()
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"series": []}
        mock_resp.raise_for_status = MagicMock()
        backend._session.get = MagicMock(return_value=mock_resp)

        result = backend.fetch("pipe_a")

        assert result.pipeline_id == "pipe_a"
        assert result.last_run is None
        assert result.row_count is None
        assert result.error_count is None
        assert result.duration_seconds is None

    def test_parses_last_run_timestamp(self):
        backend = _make_backend()
        ts = 1_700_000_000.0

        def _get(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "last_run_ts" in kwargs.get("params", {}).get("query", ""):
                resp.json.return_value = _series_response("pipe_a", ts)
            else:
                resp.json.return_value = {"series": []}
            return resp

        backend._session.get = _get
        result = backend.fetch("pipe_a")
        expected = datetime.fromtimestamp(ts, tz=timezone.utc)
        assert result.last_run == expected

    def test_parses_row_and_error_count(self):
        backend = _make_backend()

        def _get(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            query = kwargs.get("params", {}).get("query", "")
            if "row_count" in query:
                resp.json.return_value = _series_response("p", 500.0)
            elif "error_count" in query:
                resp.json.return_value = _series_response("p", 3.0)
            else:
                resp.json.return_value = {"series": []}
            return resp

        backend._session.get = _get
        result = backend.fetch("p")
        assert result.row_count == 500
        assert result.error_count == 3

    def test_raises_on_http_error(self):
        backend = _make_backend()
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("401 Unauthorized")
        backend._session.get = MagicMock(return_value=mock_resp)

        with pytest.raises(Exception, match="401"):
            backend.fetch("pipe_x")


class TestDatadogBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        backend = _make_backend()

        def _get(url, **kwargs):
            resp = MagicMock()
            resp.ok = True
            resp.raise_for_status = MagicMock()
            if "query" in url:
                resp.json.return_value = {
                    "series": [
                        {"tags": ["pipeline_id:zebra"]},
                        {"tags": ["pipeline_id:alpha"]},
                    ]
                }
            else:
                resp.json.return_value = {"metrics": []}
            return resp

        backend._session.get = _get
        ids = backend.list_pipelines()
        assert ids == ["alpha", "zebra"]

    def test_returns_empty_list_when_no_series(self):
        backend = _make_backend()
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"metrics": [], "series": []}
        backend._session.get = MagicMock(return_value=mock_resp)
        assert backend.list_pipelines() == []
