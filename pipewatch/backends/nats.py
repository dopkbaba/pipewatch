"""NATS JetStream backend for pipewatch."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class NATSBackend(BackendBase):
    """Read pipeline metrics published to a NATS JetStream subject."""

    def __init__(
        self,
        servers: str = "nats://localhost:4222",
        subject_prefix: str = "pipewatch",
        timeout: float = 2.0,
    ) -> None:
        self._servers = servers
        self._subject_prefix = subject_prefix
        self._timeout = timeout
        self._cache: Dict[str, PipelineMetrics] = {}
        self._connect()

    def _connect(self) -> None:
        import nats  # type: ignore

        import asyncio

        async def _init() -> None:
            nc = await nats.connect(self._servers)
            js = nc.jetstream()
            sub = await js.subscribe(f"{self._subject_prefix}.>")
            try:
                while True:
                    try:
                        msg = await sub.next_msg(timeout=self._timeout)
                        data = json.loads(msg.data.decode())
                        pid = data.get("pipeline_id", "")
                        if pid:
                            self._cache[pid] = self._parse_metrics(data)
                    except Exception:
                        break
            finally:
                await nc.drain()

        try:
            asyncio.get_event_loop().run_until_complete(_init())
        except Exception:
            pass

    def _parse_metrics(self, data: Dict[str, Any]) -> PipelineMetrics:
        raw_ts = data.get("last_run")
        last_run: Optional[datetime] = None
        if raw_ts:
            dt = datetime.fromisoformat(raw_ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            last_run = dt
        return PipelineMetrics(
            pipeline_id=data.get("pipeline_id", ""),
            last_run=last_run,
            error_count=data.get("error_count"),
            row_count=data.get("row_count"),
        )

    def list_pipelines(self) -> List[str]:
        return sorted(self._cache.keys())

    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        return self._cache.get(
            pipeline_id, PipelineMetrics(pipeline_id=pipeline_id)
        )
