"""Tests for TimescaleDB backend registration."""
from __future__ import annotations

from pipewatch.backends import get_backend, available_backends

import pipewatch.backends.timescaledb_register  # noqa: F401  trigger registration


def test_timescaledb_backend_is_registered():
    assert "timescaledb" in available_backends()


def test_factory_creates_timescaledb_backend_with_defaults():
    from pipewatch.backends.timescaledb import TimescaleDBBackend
    backend = get_backend("timescaledb", {})
    assert isinstance(backend, TimescaleDBBackend)
    assert backend._table == "pipeline_metrics"


def test_factory_passes_custom_config():
    from pipewatch.backends.timescaledb import TimescaleDBBackend
    backend = get_backend(
        "timescaledb",
        {"host": "db.example.com", "port": "5433", "dbname": "etl", "table": "metrics"},
    )
    assert isinstance(backend, TimescaleDBBackend)
    assert "db.example.com" in backend._dsn
    assert "5433" in backend._dsn
    assert "etl" in backend._dsn
    assert backend._table == "metrics"
