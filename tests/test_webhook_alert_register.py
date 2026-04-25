"""Tests for the webhook_alert backend registration helper."""
from __future__ import annotations

import importlib

import pytest

from pipewatch.backends import available_backends, get_backend
from pipewatch.backends.webhook_alert import WebhookAlertBackend


def _import_register():
    import pipewatch.backends.webhook_alert_register  # noqa: F401
    importlib.invalidate_caches()


def test_webhook_alert_backend_is_registered():
    _import_register()
    assert "webhook_alert" in available_backends()


def test_factory_creates_webhook_alert_backend_with_defaults():
    _import_register()
    factory = get_backend("webhook_alert")
    backend = factory({"url": "https://hooks.example.com/notify"})
    assert isinstance(backend, WebhookAlertBackend)
    assert backend._cfg.url == "https://hooks.example.com/notify"
    assert backend._cfg.method == "POST"
    assert backend._cfg.timeout == 10
    assert backend._cfg.headers == {}
    assert backend._cfg.extra == {}


def test_factory_passes_custom_config():
    _import_register()
    factory = get_backend("webhook_alert")
    backend = factory(
        {
            "url": "https://hooks.example.com/custom",
            "method": "PUT",
            "headers": {"Authorization": "Bearer tok"},
            "timeout": 5,
            "extra": {"env": "staging"},
        }
    )
    assert backend._cfg.method == "PUT"
    assert backend._cfg.headers == {"Authorization": "Bearer tok"}
    assert backend._cfg.timeout == 5
    assert backend._cfg.extra == {"env": "staging"}


def test_factory_raises_when_url_missing():
    _import_register()
    factory = get_backend("webhook_alert")
    with pytest.raises(ValueError, match="url"):
        factory({})
