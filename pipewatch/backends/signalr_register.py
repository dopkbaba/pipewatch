"""Register the SignalR backend with pipewatch's backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.signalr import SignalRBackend

    return SignalRBackend(
        base_url=config.get("base_url", "http://localhost:5000"),
        hub=config.get("hub", "pipewatch"),
        timeout=int(config.get("timeout", 10)),
    )


register_backend("signalr", _factory)
