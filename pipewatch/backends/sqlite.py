"""SQLite backend for pipewatch — stores pipeline metrics in a local SQLite database."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class SqliteBackend(BackendBase):
    """Read pipeline metrics from a SQLite database.

    Expected schema::

        CREATE TABLE pipeline_metrics (
            pipeline_id  TEXT    NOT NULL,
            last_run     TEXT,        -- ISO-8601 UTC timestamp, nullable
            error_rate   REAL,        -- 0.0 – 1.0, nullable
            row_count    INTEGER,     -- nullable
            PRIMARY KEY (pipeline_id)
        );
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"SQLite database not found: {self.db_path}")

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _parse_ts(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(
                f"Invalid ISO-8601 timestamp in database: {value!r}"
            ) from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    # ------------------------------------------------------------------
    # BackendBase interface
    # ------------------------------------------------------------------

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_run, error_rate, row_count "
                "FROM pipeline_metrics WHERE pipeline_id = ?",
                (pipeline_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"Pipeline not found in SQLite backend: {pipeline_id!r}")
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(row["last_run"]),
            error_rate=row["error_rate"],
            row_count=row["row_count"],
        )

    def list_pipelines(self) -> List[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT pipeline_id FROM pipeline_metrics ORDER BY pipeline_id"
            ).fetchall()
        return [r["pipeline_id"] for r in rows]
