"""Register the MQTT backend with pipewatch's backend registry."""
from __future__ import annotations

from typing import Any, Dict


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.mqtt import MQTTBackend

    return MQTTBackend(
        host=config.get("host", "localhost"),
        port=int(config.get("port", 1883)),
        topic=config.get("topic", "pipewatch/#"),
        keepalive=int(config.get("keepalive", 60)),
    )


try:
    from pipewatch.backends import register_backend

    register_backend("mqtt", _factory)
except Exception:  # pragma: no cover
    pass
