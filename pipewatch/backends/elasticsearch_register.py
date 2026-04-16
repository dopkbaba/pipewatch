"""Register the Elasticsearch backend with the pipewatch backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.elasticsearch import ElasticsearchBackend

    return ElasticsearchBackend(
        hosts=config.get("hosts"),
        index=config.get("index", "pipewatch"),
    )


register_backend("elasticsearch", _factory)
