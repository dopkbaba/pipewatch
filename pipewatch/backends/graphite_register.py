"""Register the Graphite backend with pipewatch."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.graphite import GraphiteBackend

    return GraphiteBackend(
        base_url=config.get("base_url", "http://localhost:80"),
        prefix=config.get("prefix", "pipewatch"),
        timeout=int(config.get("timeout", 10)),
    )


register_backend("graphite", _factory)
