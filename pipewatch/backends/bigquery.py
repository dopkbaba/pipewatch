"""BigQuery backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class BigQueryBackend(BackendBase):
    """Fetch pipeline metrics from a BigQuery table."""

    def __init__(
        self,
        project: str,
        dataset: str,
        table: str = "pipeline_metrics",
        credentials=None,
    ) -> None:
        self.project = project
        self.dataset = dataset
        self.table = table
        self._credentials = credentials
        self._client = None

    def _connect(self):
        if self._client is None:
            from google.cloud import bigquery  # type: ignore

            self._client = bigquery.Client(
                project=self.project, credentials=self._credentials
            )
        return self._client

    @staticmethod
    def _parse_ts(value) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        return datetime.fromisoformat(str(value)).replace(tzinfo=timezone.utc)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        client = self._connect()
        full_table = f"`{self.project}.{self.dataset}.{self.table}`"
        query = (
            f"SELECT last_run, row_count, error_rate "
            f"FROM {full_table} "
            f"WHERE pipeline_id = @pid "
            f"ORDER BY last_run DESC LIMIT 1"
        )
        from google.cloud import bigquery as bq  # type: ignore

        job_config = bq.QueryJobConfig(
            query_parameters=[bq.ScalarQueryParameter("pid", "STRING", pipeline_id)]
        )
        rows = list(client.query(query, job_config=job_config).result())
        if not rows:
            return PipelineMetrics(pipeline_id=pipeline_id)
        row = rows[0]
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(row.last_run),
            row_count=row.row_count,
            error_rate=float(row.error_rate) if row.error_rate is not None else None,
        )

    def list_pipelines(self) -> List[str]:
        client = self._connect()
        full_table = f"`{self.project}.{self.dataset}.{self.table}`"
        query = f"SELECT DISTINCT pipeline_id FROM {full_table} ORDER BY pipeline_id"
        rows = list(client.query(query).result())
        return [row.pipeline_id for row in rows]
