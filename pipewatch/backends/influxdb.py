"""InfluxDB backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class InfluxDBBackend(BackendBase):
    """Fetch pipeline metrics from an InfluxDB v2 bucket."""

    def __init__(
        self,
        url: str = "http://localhost:8086",
        token: str = "",
        org: str = "pipewatch",
        bucket: str = "pipelines",
        measurement: str = "pipeline_health",
    ) -> None:
        self._url = url
        self._token = token
        self._org = org
        self._bucket = bucket
        self._measurement = measurement
        self._client = self._connect()

    def _connect(self):
        from influxdb_client import InfluxDBClient  # type: ignore

        return InfluxDBClient(url=self._url, token=self._token, org=self._org)

    @staticmethod
    def _parse_ts(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        query_api = self._client.query_api()
        flux = (
            f'from(bucket:"{self._bucket}")'
            f" |> range(start: -7d)"
            f' |> filter(fn: (r) => r._measurement == "{self._measurement}")'
            f' |> filter(fn: (r) => r.pipeline_id == "{pipeline_id}")'
            f" |> last()"
        )
        tables = query_api.query(flux)
        fields: dict = {}
        for table in tables:
            for record in table.records:
                fields[record.get_field()] = record.get_value()

        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(fields.get("last_run")),
            records_processed=fields.get("records_processed"),
            error_count=fields.get("error_count"),
        )

    def list_pipelines(self) -> List[str]:
        query_api = self._client.query_api()
        flux = (
            f'from(bucket:"{self._bucket}")'
            f" |> range(start: -7d)"
            f' |> filter(fn: (r) => r._measurement == "{self._measurement}")'
            f" |> keep(columns: [\"pipeline_id\"])"
            f" |> distinct(column: \"pipeline_id\")"
        )
        tables = query_api.query(flux)
        ids = set()
        for table in tables:
            for record in table.records:
                ids.add(record["pipeline_id"])
        return sorted(ids)
