"""Auto-registers the postgres backend with the pipewatch backend registry."""
from __future__ import annotations

from typing import Any

from pipewatch.backends import register_backend


def _factory(config: dict[str, Any]) -> "PostgresBackend":  # noqa: F821
    """Instantiate a :class:`PostgresBackend` from a config mapping.

    Required keys
    -------------
    dsn : str
        A libpq connection string, e.g.
        ``"postgresql://user:pass@localhost:5432/mydb"``.

    Optional keys
    -------------
    table : str
        Name of the metrics table (default: ``"pipeline_metrics"``).
    """
    from pipewatch.backends.postgres import PostgresBackend

    dsn: str = config["dsn"]
    table: str = config.get("table", "pipeline_metrics")
    return PostgresBackend(dsn=dsn, table=table)


register_backend("postgres", _factory)
