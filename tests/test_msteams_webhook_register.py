"""Tests for the MSTeams Webhook backend registration."""
from __future__ import annotations

import pytest


def _import_register():
    import importlib
    return importlib.import_module("pipewatch.backends.msteams_webhook_register")


def test_msteams_webhook_backend_is_registered():
    _import_register()
    from pipewatch.backends import available_backends
    assert "msteams_webhook" in available_backends()


def test_factory_creates_backend_with_defaults():
    _import_register()
    from pipewatch.backends import get_backend
    from pipewatch.backends.msteams_webhook import MSTeamsWebhookAlertBackend

    backend = get_backend("msteams_webhook", {"webhook_url": "https://x.example.com/hook"})
    assert isinstance(backend, MSTeamsWebhookAlertBackend)


def test_factory_passes_custom_config():
    _import_register()
    from pipewatch.backends import get_backend

    backend = get_backend(
        "msteams_webhook",
        {
            "webhook_url": "https://x.example.com/hook",
            "mention_email": "ops@example.com",
            "timeout": "15",
            "title_prefix": "[STAGING]",
        },
    )
    assert backend._cfg.mention_email == "ops@example.com"
    assert backend._cfg.timeout == 15
    assert backend._cfg.title_prefix == "[STAGING]"


def test_factory_missing_webhook_url_raises():
    _import_register()
    from pipewatch.backends import get_backend

    with pytest.raises(KeyError):
        get_backend("msteams_webhook", {})
