"""Tests for the RabbitMQ backend registration."""
from __future__ import annotations

from pipewatch.backends import available_backends, get_backend
from pipewatch.backends.rabbitmq import RabbitMQBackend
import pipewatch.backends.rabbitmq_register  # noqa: F401  ensure side-effects run


def test_rabbitmq_backend_is_registered():
    assert "rabbitmq" in available_backends()


def test_factory_creates_rabbitmq_backend_with_defaults():
    backend = get_backend("rabbitmq", {})
    assert isinstance(backend, RabbitMQBackend)
    assert backend._host == "localhost"
    assert backend._port == 5672
    assert backend._queue == "pipewatch"
    assert backend._username == "guest"
    assert backend._password == "guest"
    assert backend._max_messages == 200


def test_factory_passes_custom_config():
    backend = get_backend("rabbitmq", {
        "host": "rmq.internal",
        "port": "5673",
        "queue": "etl_health",
        "username": "admin",
        "password": "secret",
        "max_messages": "50",
    })
    assert backend._host == "rmq.internal"
    assert backend._port == 5673
    assert backend._queue == "etl_health"
    assert backend._username == "admin"
    assert backend._password == "secret"
    assert backend._max_messages == 50
