"""Register the MySQL backend with the pipewatch backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    """Instantiate a :class:`MySQLBackend` from a config dict.

    Supported keys (all optional, fall back to driver defaults):
        host, port, user, password, database, table
    """
    from pipewatch.backends.mysql import MySQLBackend

    return MySQLBackend(
        host=config.get("host", "localhost"),
        port=int(config.get("port", 3306)),
        user=config.get("user", "root"),
        password=config.get("password", ""),
        database=config.get("database", "pipewatch"),
        table=config.get("table", "pipeline_metrics"),
    )


register_backend("mysql", _factory)
