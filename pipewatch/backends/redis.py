"""Redis backend for pipewatch — reads pipeline metrics from Redis hashes."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class RedisBackend(BackendBase):
    """Fetch pipeline metrics stored as Redis hashes.

    Expected key pattern: ``<prefix><pipeline_id>``
    Expected hash fields: ``last_run`` (ISO-8601), ``error_rate`` (float),
    ``rows_processed`` (int).
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        prefix: str = "pipewatch:",
    ) -> None:
        try:
            import redis  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "redis-py is required for RedisBackend: pip install redis"
            ) from exc

        self._redis = redis.Redis(
            host=host, port=port, db=db, password=password, decode_responses=True
        )
        self._prefix = prefix

    # ------------------------------------------------------------------
    # BackendBase interface
    # ------------------------------------------------------------------

    def list_pipelines(self) -> List[str]:
        """Return sorted list of pipeline IDs found in Redis."""
        keys = self._redis.keys(f"{self._prefix}*")
        prefix_len = len(self._prefix)
        return sorted(k[prefix_len:] for k in keys)

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        """Return metrics for *pipeline_id* from its Redis hash."""
        key = f"{self._prefix}{pipeline_id}"
        data = self._redis.hgetall(key)

        last_run: Optional[datetime] = None
        if data.get("last_run"):
            dt = datetime.fromisoformat(data["last_run"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            last_run = dt

        error_rate: Optional[float] = None
        if data.get("error_rate") is not None:
            try:
                error_rate = float(data["error_rate"])
            except ValueError:
                pass

        rows_processed: Optional[int] = None
        if data.get("rows_processed") is not None:
            try:
                rows_processed = int(data["rows_processed"])
            except ValueError:
                pass

        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=last_run,
            error_rate=error_rate,
            rows_processed=rows_processed,
        )
