"""Tests for the MongoDB backend registration."""
from __future__ import annotations

import pytest

from pipewatch.backends import available_backends, get_backend


def _import_mongodb_register():
    """Import the MongoDB registration module to trigger backend registration."""
    import pipewatch.backends.mongodb_register  # noqa: F401


def test_mongodb_backend_is_registered():
    _import_mongodb_register()
    assert "mongodb" in available_backends()


def test_factory_creates_mongodb_backend_with_defaults():
    _import_mongodb_register()
    from pipewatch.backends.mongodb import MongoDBBackend

    factory = get_backend("mongodb")
    backend = factory({})
    assert isinstance(backend, MongoDBBackend)
    assert backend._uri == "mongodb://localhost:27017"
    assert backend._database == "pipewatch"
    assert backend._collection == "pipeline_metrics"


def test_factory_passes_custom_config():
    _import_mongodb_register()
    from pipewatch.backends.mongodb import MongoDBBackend

    factory = get_backend("mongodb")
    backend = factory({
        "uri": "mongodb://remotehost:27017",
        "database": "mydb",
        "collection": "metrics",
    })
    assert isinstance(backend, MongoDBBackend)
    assert backend._uri == "mongodb://remotehost:27017"
    assert backend._database == "mydb"
    assert backend._collection == "metrics"


def test_factory_partial_config_uses_defaults_for_missing_keys():
    """Verify that omitted config keys fall back to their default values."""
    _import_mongodb_register()
    from pipewatch.backends.mongodb import MongoDBBackend

    factory = get_backend("mongodb")
    backend = factory({"database": "custom_db"})
    assert isinstance(backend, MongoDBBackend)
    assert backend._uri == "mongodb://localhost:27017"
    assert backend._database == "custom_db"
    assert backend._collection == "pipeline_metrics"
