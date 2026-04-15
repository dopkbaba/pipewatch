"""MySQL backend for pipewatch — reads pipeline metrics from a MySQL/MariaDB table."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class MySQLBackend(BackendBase):
    """Fetch pipeline metrics from a MySQL database.

    Expected table schema (example)::

        CREATE TABLE pipeline_metrics (
            pipeline_id   VARCHAR(255) PRIMARY KEY,
            last_run      DATETIME,
            error_rate    FLOAT,
            row_count     BIGINT
        );
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "pipewatch",
        table: str = "pipeline_metrics",
    ) -> None:
        self._dsn = dict(
            host=host, port=port, user=user, password=password, database=database
        )
        self._table = table
        self._conn = None

    # ------------------------------------------------------------------
    def _connect(self):
        """Lazy-connect so import errors surface only when backend is used."""
        if self._conn is None:
            import mysql.connector  # type: ignore

            self._conn = mysql.connector.connect(**self._dsn)
        return self._conn

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_ts(value) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        # Fallback: parse ISO string
        dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    # ------------------------------------------------------------------
    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        conn = self._connect()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT last_run, error_rate, row_count"
            f" FROM `{self._table}` WHERE pipeline_id = %s",
            (pipeline_id,),
        )
        row = cur.fetchone()
        cur.close()
        if row is None:
            return PipelineMetrics(pipeline_id=pipeline_id)
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(row.get("last_run")),
            error_rate=row.get("error_rate"),
            row_count=row.get("row_count"),
        )

    def list_pipelines(self) -> List[str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SELECT pipeline_id FROM `{self._table}` ORDER BY pipeline_id")
        ids = [r[0] for r in cur.fetchall()]
        cur.close()
        return ids
