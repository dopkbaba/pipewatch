"""Tests for NATS backend registration."""
from __future__ import annotations

from unittest.mock import patch

from pipewatch.backends.nats import NATSBackend


def test_nats_backend_is_registered():
    import pipewatch.backends.nats_register  # noqa: F401
    from pipewatch.backends import available_backends

    assert "nats" in available_backends()


def test_factory_creates_nats_backend_with_defaults():
    import pipewatch.backends.nats_register  # noqa: F401
    from pipewatch.backends import get_backend

    with patch.object(NATSBackend, "_connect", return_value=None):
        backend = get_backend("nats", {})

    assert isinstance(backend, NATSBackend)
    assert backend._servers == "nats://localhost:4222"
    assert backend._subject_prefix == "pipewatch"
    assert backend._timeout == 2.0


def test_factory_passes_custom_config():
    import pipewatch.backends.nats_register  # noqa: F401
    from pipewatch.backends import get_backend

    with patch.object(NATSBackend, "_connect", return_value=None):
        backend = get_backend("nats", {
            "servers": "nats://remote:4222",
            "subject_prefix": "etl",
            "timeout": "5",
        })

    assert backend._servers == "nats://remote:4222"
    assert backend._subject_prefix == "etl"
    assert backend._timeout == 5.0
