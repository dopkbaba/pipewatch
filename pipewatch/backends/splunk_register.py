"""Register the Splunk backend with pipewatch's backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.splunk import SplunkBackend

    return SplunkBackend(
        base_url=config.get("base_url", "http://localhost:8089"),
        token=config.get("token", ""),
        index=config.get("index", "pipewatch"),
        verify_ssl=config.get("verify_ssl", True),
        timeout=int(config.get("timeout", 10)),
    )


register_backend("splunk", _factory)
