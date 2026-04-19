"""Register the RabbitMQ backend with the pipewatch backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend
from pipewatch.backends.rabbitmq import RabbitMQBackend


def _factory(config: Dict[str, Any]) -> RabbitMQBackend:
    return RabbitMQBackend(
        host=config.get("host", "localhost"),
        port=int(config.get("port", 5672)),
        queue=config.get("queue", "pipewatch"),
        username=config.get("username", "guest"),
        password=config.get("password", "guest"),
        max_messages=int(config.get("max_messages", 200)),
    )


register_backend("rabbitmq", _factory)
