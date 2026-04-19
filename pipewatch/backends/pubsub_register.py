"""Register the Pub/Sub backend with the pipewatch backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.pubsub import PubSubBackend

    return PubSubBackend(
        project=config["project"],
        subscription=config.get("subscription", "pipewatch"),
        max_messages=int(config.get("max_messages", 100)),
    )


register_backend("pubsub", _factory)
