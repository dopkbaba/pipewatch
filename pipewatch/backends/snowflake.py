"""Snowflake backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class SnowflakeBackend(BackendBase):
    """Fetch pipeline metrics from a Snowflake table."""

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        database: str,
        schema: str = "PUBLIC",
        warehouse: str = "COMPUTE_WH",
        table: str = "pipeline_metrics",
    ) -> None:
        self._dsn: Dict[str, Any] = dict(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
        )
        self._table = table
        self._conn: Any = None

    def _connect(self) -> Any:
        if self._conn is None:
            import snowflake.connector  # type: ignore

            self._conn = snowflake.connector.connect(**self._dsn)
        return self._conn

    @staticmethod
    def _parse_ts(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            f"SELECT last_run, row_count, error_rate "
            f"FROM {self._table} WHERE pipeline_id = %s "
            f"ORDER BY last_run DESC LIMIT 1",
            (pipeline_id,),
        )
        row = cur.fetchone()
        if row is None:
            return PipelineMetrics(pipeline_id=pipeline_id)
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(row[0]),
            row_count=row[1],
            error_rate=float(row[2]) if row[2] is not None else None,
        )

    def list_pipelines(self) -> List[str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT pipeline_id FROM {self._table} ORDER BY pipeline_id")
        return [r[0] for r in cur.fetchall()]
