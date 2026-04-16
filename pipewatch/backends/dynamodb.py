"""DynamoDB backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class DynamoDBBackend(BackendBase):
    """Reads pipeline metrics from a DynamoDB table.

    Expected item schema:
        pipeline_id (S) – partition key
        last_run     (S) – ISO-8601 timestamp (optional)
        error_rate   (N) – float (optional)
        row_count    (N) – int (optional)
    """

    def __init__(
        self,
        table_name: str,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
    ) -> None:
        self.table_name = table_name
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self._table = None

    def _connect(self):
        if self._table is None:
            import boto3

            dynamodb = boto3.resource(
                "dynamodb",
                region_name=self.region_name,
                endpoint_url=self.endpoint_url,
            )
            self._table = dynamodb.Table(self.table_name)
        return self._table

    @staticmethod
    def _parse_ts(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        table = self._connect()
        response = table.get_item(Key={"pipeline_id": pipeline_id})
        item = response.get("Item")
        if not item:
            return PipelineMetrics(pipeline_id=pipeline_id)
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(item.get("last_run")),
            error_rate=float(item["error_rate"]) if "error_rate" in item else None,
            row_count=int(item["row_count"]) if "row_count" in item else None,
        )

    def list_pipelines(self) -> List[str]:
        table = self._connect()
        response = table.scan(ProjectionExpression="pipeline_id")
        ids = [item["pipeline_id"] for item in response.get("Items", [])]
        return sorted(ids)
