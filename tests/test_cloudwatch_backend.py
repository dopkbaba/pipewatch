"""Tests for the CloudWatch backend."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.backends.cloudwatch import CloudWatchBackend


def _make_backend(pages=None, datapoints=None):
    client = MagicMock()

    # list_metrics paginator
    paginator = MagicMock()
    paginator.paginate.return_value = iter(pages or [])
    client.get_paginator.return_value = paginator

    # get_metric_statistics
    client.get_metric_statistics.return_value = {"Datapoints": datapoints or []}

    return CloudWatchBackend(client=client)


class TestCloudWatchBackendListPipelines:
    def test_returns_sorted_pipeline_ids(self):
        pages = [
            {
                "Metrics": [
                    {"Dimensions": [{"Name": "pipeline_id", "Value": "pipe_b"}]},
                    {"Dimensions": [{"Name": "pipeline_id", "Value": "pipe_a"}]},
                ]
            }
        ]
        backend = _make_backend(pages=pages)
        assert backend.list_pipelines() == ["pipe_a", "pipe_b"]

    def test_returns_empty_when_no_metrics(self):
        backend = _make_backend(pages=[{"Metrics": []}])
        assert backend.list_pipelines() == []

    def test_ignores_non_pipeline_dimensions(self):
        pages = [
            {
                "Metrics": [
                    {"Dimensions": [{"Name": "env", "Value": "prod"}]},
                ]
            }
        ]
        backend = _make_backend(pages=pages)
        assert backend.list_pipelines() == []


class TestCloudWatchBackendFetch:
    def test_returns_empty_metrics_when_no_datapoints(self):
        backend = _make_backend(datapoints=[])
        result = backend.fetch("pipe_x")
        assert result.pipeline_id == "pipe_x"
        assert result.last_run is None
        assert result.error_rate is None
        assert result.row_count is None

    def test_parses_aware_datetime(self):
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp()
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = iter([])
        client.get_paginator.return_value = paginator

        def _stat_side_effect(**kwargs):
            name = kwargs["MetricName"]
            if name == "last_run_timestamp":
                return {
                    "Datapoints": [
                        {"Maximum": ts, "Timestamp": datetime(2024, 1, 15, tzinfo=timezone.utc)}
                    ]
                }
            return {"Datapoints": []}

        client.get_metric_statistics.side_effect = _stat_side_effect
        backend = CloudWatchBackend(client=client)
        result = backend.fetch("pipe_y")
        assert result.last_run is not None
        assert result.last_run.tzinfo is not None

    def test_parses_row_count_as_int(self):
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = iter([])
        client.get_paginator.return_value = paginator

        def _stat_side_effect(**kwargs):
            name = kwargs["MetricName"]
            if name == "row_count":
                return {
                    "Datapoints": [
                        {"Maximum": 42.0, "Timestamp": datetime(2024, 1, 15, tzinfo=timezone.utc)}
                    ]
                }
            return {"Datapoints": []}

        client.get_metric_statistics.side_effect = _stat_side_effect
        backend = CloudWatchBackend(client=client)
        result = backend.fetch("pipe_z")
        assert result.row_count == 42
