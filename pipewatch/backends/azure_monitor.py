"""Azure Monitor backend for pipewatch."""
from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class AzureMonitorBackend(BackendBase):
    """Fetch pipeline metrics from Azure Monitor."""

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_id: str,
        credential: Any = None,
        timespan: str = "PT1H",
    ) -> None:
        self._subscription_id = subscription_id
        self._resource_group = resource_group
        self._workspace_id = workspace_id
        self._credential = credential
        self._timespan = timespan
        self._client = self._connect()

    # ------------------------------------------------------------------
    def _connect(self) -> Any:
        """Return an Azure Monitor query client (lazy import)."""
        try:
            from azure.monitor.query import LogsQueryClient  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "azure-monitor-query is required for AzureMonitorBackend"
            ) from exc
        return LogsQueryClient(self._credential)

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_ts(value: Optional[str]) -> Optional[datetime.datetime]:
        if not value:
            return None
        dt = datetime.datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt

    # ------------------------------------------------------------------
    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        query = (
            f"PipelineMetrics "
            f"| where pipeline_id == '{pipeline_id}' "
            f"| order by TimeGenerated desc "
            f"| take 1"
        )
        result = self._client.query_workspace(
            workspace_id=self._workspace_id,
            query=query,
            timespan=self._timespan,
        )
        rows: List[Any] = []
        for table in result.tables:
            rows.extend(table.rows)
        if not rows:
            return PipelineMetrics(pipeline_id=pipeline_id)
        row: Dict[str, Any] = dict(zip(result.tables[0].columns, rows[0]))
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(row.get("last_run")),
            record_count=row.get("record_count"),
            error_count=row.get("error_count"),
        )

    def list_pipelines(self) -> List[str]:
        query = (
            "PipelineMetrics "
            "| summarize by pipeline_id "
            "| order by pipeline_id asc"
        )
        result = self._client.query_workspace(
            workspace_id=self._workspace_id,
            query=query,
            timespan=self._timespan,
        )
        ids: List[str] = []
        for table in result.tables:
            for row in table.rows:
                ids.append(str(row[0]))
        return sorted(ids)
