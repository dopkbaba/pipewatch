"""Tests for the MongoDB backend registration."""
from __future__ import annotations

import pytest

from pipewatch.backends import available_backends, get_backend


def test_mongodb_backend_is_registered():
    import pipewatch.backends.mongodb_register  # noqa: F401
    assert "mongodb" in available_backends()


def test_factory_creates_mongodb_backend_with_defaults():
    import pipewatch.backends.mongodb_register  # noqa: F401
    from pipewatch.backends.mongodb import MongoDBBackend

    factory = get_backend("mongodb")
    backend = factory({})
    assert isinstance(backend, MongoDBBackend)
    assert backend._uri == "mongodb://localhost:27017"
    assert backend._database == "pipewatch"
    assert backend._collection == "pipeline_metrics"


def test_factory_passes_custom_config():
    import pipewatch.backends.mongodb_register  # noqa: F401
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
