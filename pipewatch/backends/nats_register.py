"""Register the NATS backend with the pipewatch backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend
from pipewatch.backends.nats import NATSBackend


def _factory(config: Dict[str, Any]) -> NATSBackend:
    return NATSBackend(
        servers=config.get("servers", "nats://localhost:4222"),
        subject_prefix=config.get("subject_prefix", "pipewatch"),
        timeout=float(config.get("timeout", 2.0)),
    )


register_backend("nats", _factory)
