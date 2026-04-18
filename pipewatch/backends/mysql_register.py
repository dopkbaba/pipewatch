"""Register the MySQL backend with the pipewatch backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    """Instantiate a :class:`MySQLBackend` from a config dict.

    Supported keys (all optional, fall back to driver defaults):
        host, port, user, password, database, table

    Raises:
        ValueError: If ``port`` is present but cannot be converted to an integer.
    """
    from pipewatch.backends.mysql import MySQLBackend

    port_raw = config.get("port", 3306)
    try:
        port = int(port_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid value for MySQL 'port': {port_raw!r}. Expected an integer."
        ) from exc

    return MySQLBackend(
        host=config.get("host", "localhost"),
        port=port,
        user=config.get("user", "root"),
        password=config.get("password", ""),
        database=config.get("database", "pipewatch"),
        table=config.get("table", "pipeline_metrics"),
    )


register_backend("mysql", _factory)
