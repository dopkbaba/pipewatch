"""Register TimescaleDB backend with pipewatch backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.timescaledb import TimescaleDBBackend

    return TimescaleDBBackend(
        host=config.get("host", "localhost"),
        port=int(config.get("port", 5432)),
        dbname=config.get("dbname", "pipewatch"),
        user=config.get("user", "postgres"),
        password=config.get("password", ""),
        table=config.get("table", "pipeline_metrics"),
    )


register_backend("timescaledb", _factory)
