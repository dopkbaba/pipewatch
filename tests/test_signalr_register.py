"""Tests for the SignalR backend registration."""
from __future__ import annotations

import importlib

import pytest


def _import_signalr_register():
    import pipewatch.backends.signalr_register  # noqa: F401
    from pipewatch.backends import get_backend
    return get_backend


def test_signalr_backend_is_registered():
    get_backend = _import_signalr_register()
    from pipewatch.backends import available_backends
    assert "signalr" in available_backends()


def test_factory_creates_signalr_backend_with_defaults():
    get_backend = _import_signalr_register()
    from pipewatch.backends.signalr import SignalRBackend
    backend = get_backend("signalr", {})
    assert isinstance(backend, SignalRBackend)
    assert backend._base_url == "http://localhost:5000"
    assert backend._hub == "pipewatch"
    assert backend._timeout == 10


def test_factory_passes_custom_config():
    get_backend = _import_signalr_register()
    from pipewatch.backends.signalr import SignalRBackend
    cfg = {"base_url": "https://my.signalr.io", "hub": "etl", "timeout": "30"}
    backend = get_backend("signalr", cfg)
    assert isinstance(backend, SignalRBackend)
    assert backend._base_url == "https://my.signalr.io"
    assert backend._hub == "etl"
    assert backend._timeout == 30
