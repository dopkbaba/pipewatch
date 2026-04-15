"""PostgreSQL backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics

try:
    import psycopg2
    import psycopg2.extras
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "psycopg2 is required for the postgres backend: pip install psycopg2-binary"
    ) from exc


class PostgresBackend(BackendBase):
    """Read pipeline metrics from a PostgreSQL database.

    Expected table schema::

        CREATE TABLE pipeline_metrics (
            pipeline_id  TEXT        NOT NULL,
            last_run     TIMESTAMPTZ,
            row_count    BIGINT,
            error_count  BIGINT,
            duration_sec DOUBLE PRECISION
        );
    """

    def __init__(
        self,
        dsn: str,
        table: str = "pipeline_metrics",
    ) -> None:
        self._dsn = dsn
        self._table = table
        self._conn: Optional[psycopg2.extensions.connection] = None

    def _connect(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._dsn)
        return self._conn

    @staticmethod
    def _parse_ts(value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        conn = self._connect()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                f"SELECT last_run, row_count, error_count, duration_sec "
                f"FROM {self._table} WHERE pipeline_id = %s "
                f"ORDER BY last_run DESC NULLS LAST LIMIT 1",
                (pipeline_id,),
            )
            row = cur.fetchone()
        if row is None:
            return PipelineMetrics(pipeline_id=pipeline_id)
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(row["last_run"]),
            row_count=row["row_count"],
            error_count=row["error_count"],
            duration_sec=row["duration_sec"],
        )

    def list_pipelines(self) -> List[str]:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT pipeline_id FROM {self._table} ORDER BY pipeline_id"
            )
            rows = cur.fetchall()
        return [r[0] for r in rows]
