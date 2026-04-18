"""TimescaleDB backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class TimescaleDBBackend(BackendBase):
    """Fetch pipeline metrics from a TimescaleDB (PostgreSQL) instance."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        dbname: str = "pipewatch",
        user: str = "postgres",
        password: str = "",
        table: str = "pipeline_metrics",
    ) -> None:
        self._dsn = (
            f"host={host} port={port} dbname={dbname} "
            f"user={user} password={password}"
        )
        self._table = table
        self._conn: Any = None

    def _connect(self) -> Any:
        if self._conn is None:
            import psycopg2  # type: ignore
            self._conn = psycopg2.connect(self._dsn)
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
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT last_run, record_count, error_count
                FROM {self._table}
                WHERE pipeline_id = %s
                ORDER BY last_run DESC
                LIMIT 1
                """,
                (pipeline_id,),
            )
            row = cur.fetchone()
        if row is None:
            return PipelineMetrics(pipeline_id=pipeline_id)
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(row[0]),
            record_count=row[1],
            error_count=row[2],
        )

    def list_pipelines(self) -> List[str]:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT pipeline_id FROM {self._table} ORDER BY pipeline_id"
            )
            rows = cur.fetchall()
        return [r[0] for r in rows]
