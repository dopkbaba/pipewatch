"""Tests for the MySQL backend factory registration."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends import get_backend, available_backends


def test_mysql_backend_is_registered():
    """After importing the register module the 'mysql' key must be present."""
    import pipewatch.backends.mysql_register  # noqa: F401 — side-effect import

    assert "mysql" in available_backends()


def test_factory_creates_mysql_backend_with_defaults():
    import pipewatch.backends.mysql_register  # noqa: F401

    from pipewatch.backends.mysql import MySQLBackend

    backend = get_backend("mysql", {})
    assert isinstance(backend, MySQLBackend)
    assert backend._dsn["host"] == "localhost"
    assert backend._dsn["port"] == 3306
    assert backend._table == "pipeline_metrics"


def test_factory_passes_custom_config():
    import pipewatch.backends.mysql_register  # noqa: F401

    from pipewatch.backends.mysql import MySQLBackend

    cfg = {
        "host": "mysql-host",
        "port": "3307",
        "user": "admin",
        "password": "secret",
        "database": "etl_db",
        "table": "metrics",
    }
    backend = get_backend("mysql", cfg)
    assert isinstance(backend, MySQLBackend)
    assert backend._dsn["host"] == "mysql-host"
    assert backend._dsn["port"] == 3307  # cast to int
    assert backend._dsn["user"] == "admin"
    assert backend._dsn["password"] == "secret"
    assert backend._dsn["database"] == "etl_db"
    assert backend._table == "metrics"
