"""Register the OpenSearch backend with pipewatch."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.opensearch import OpenSearchBackend

    http_auth = None
    user = config.get("username")
    password = config.get("password")
    if user and password:
        http_auth = (user, password)

    return OpenSearchBackend(
        host=config.get("host", "localhost"),
        port=int(config.get("port", 9200)),
        index=config.get("index", "pipewatch"),
        scheme=config.get("scheme", "http"),
        http_auth=http_auth,
    )


register_backend("opensearch", _factory)
