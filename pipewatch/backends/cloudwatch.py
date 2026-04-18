"""AWS CloudWatch backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class CloudWatchBackend(BackendBase):
    """Fetch pipeline metrics from AWS CloudWatch custom metrics."""

    def __init__(
        self,
        namespace: str = "PipeWatch",
        region: str = "us-east-1",
        client=None,
    ) -> None:
        self.namespace = namespace
        self.region = region
        self._client = client

    def _connect(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("cloudwatch", region_name=self.region)
        return self._client

    def _parse_ts(self, value: Optional[float]) -> Optional[datetime]:
        if value is None:
            return None
        return datetime.fromtimestamp(value, tz=timezone.utc)

    def list_pipelines(self) -> List[str]:
        client = self._connect()
        paginator = client.get_paginator("list_metrics")
        ids: set = set()
        for page in paginator.paginate(Namespace=self.namespace, MetricName="last_run_timestamp"):
            for metric in page.get("Metrics", []):
                for dim in metric.get("Dimensions", []):
                    if dim["Name"] == "pipeline_id":
                        ids.add(dim["Value"])
        return sorted(ids)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        client = self._connect()
        dims = [{"Name": "pipeline_id", "Value": pipeline_id}]
        now = datetime.now(tz=timezone.utc)
        from datetime import timedelta
        start = now - timedelta(hours=24)

        def _latest(metric_name: str) -> Optional[float]:
            resp = client.get_metric_statistics(
                Namespace=self.namespace,
                MetricName=metric_name,
                Dimensions=dims,
                StartTime=start,
                EndTime=now,
                Period=86400,
                Statistics=["Maximum"],
            )
            points = resp.get("Datapoints", [])
            if not points:
                return None
            return max(points, key=lambda p: p["Timestamp"])["Maximum"]

        last_run_ts = _latest("last_run_timestamp")
        error_rate = _latest("error_rate")
        row_count = _latest("row_count")

        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(last_run_ts),
            error_rate=error_rate,
            row_count=int(row_count) if row_count is not None else None,
        )
