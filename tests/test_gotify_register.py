"""Tests for the Gotify backend registration helper."""
from __future__ import annotations

import importlib

import pytest


def _import_register():
    import pipewatch.backends.gotify_register  # noqa: F401
    from pipewatch.backends import get_backend
    return get_backend


def test_gotify_backend_is_registered():
    get_backend = _import_register()
    factory = get_backend("gotify")
    assert factory is not None


def test_factory_creates_gotify_backend_with_required_fields():
    get_backend = _import_register()
    factory = get_backend("gotify")
    from pipewatch.backends.gotify import GotifyAlertBackend
    backend = factory({"url": "https://gotify.example.com", "token": "tok"})
    assert isinstance(backend, GotifyAlertBackend)


def test_factory_passes_custom_priority_map():
    get_backend = _import_register()
    factory = get_backend("gotify")
    custom_map = {"critical": 10, "warning": 7, "ok": 2, "unknown": 4}
    backend = factory(
        {"url": "https://gotify.example.com", "token": "tok", "priority_map": custom_map}
    )
    assert backend._cfg.priority_map["critical"] == 10


def test_factory_uses_default_timeout():
    get_backend = _import_register()
    factory = get_backend("gotify")
    backend = factory({"url": "https://gotify.example.com", "token": "tok"})
    assert backend._cfg.timeout == 10


def test_factory_passes_custom_timeout():
    get_backend = _import_register()
    factory = get_backend("gotify")
    backend = factory({"url": "https://gotify.example.com", "token": "tok", "timeout": "30"})
    assert backend._cfg.timeout == 30


def test_factory_raises_when_url_missing():
    get_backend = _import_register()
    factory = get_backend("gotify")
    with pytest.raises(KeyError):
        factory({"token": "tok"})


def test_factory_raises_when_token_missing():
    get_backend = _import_register()
    factory = get_backend("gotify")
    with pytest.raises(KeyError):
        factory({"url": "https://gotify.example.com"})
