"""Tests for the Elasticsearch backend registration."""
from __future__ import annotations

import pytest

from pipewatch.backends import available_backends, get_backend


def test_elasticsearch_backend_is_registered():
    import pipewatch.backends.elasticsearch_register  # noqa: F401
    assert "elasticsearch" in available_backends()


def test_factory_creates_backend_with_defaults(monkeypatch):
    import pipewatch.backends.elasticsearch_register  # noqa: F401
    from pipewatch.backends.elasticsearch import ElasticsearchBackend

    backend = get_backend("elasticsearch")({})
    assert isinstance(backend, ElasticsearchBackend)
    assert backend._index == "pipewatch"
    assert backend._hosts == ["http://localhost:9200"]


def test_factory_passes_custom_config(monkeypatch):
    import pipewatch.backends.elasticsearch_register  # noqa: F401
    from pipewatch.backends.elasticsearch import ElasticsearchBackend

    cfg = {"hosts": ["http://es-host:9200"], "index": "custom_index"}
    backend = get_backend("elasticsearch")(cfg)
    assert isinstance(backend, ElasticsearchBackend)
    assert backend._index == "custom_index"
    assert backend._hosts == ["http://es-host:9200"]
