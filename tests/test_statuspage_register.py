"""Tests for the Statuspage.io backend registration."""
from __future__ import annotations

import pytest


def test_statuspage_backend_is_registered():
    from pipewatch.backends import available_backends
    import pipewatch.backends.statuspage_register  # noqa: F401

    assert "statuspage" in available_backends()


def test_factory_creates_statuspage_backend_with_defaults():
    import pipewatch.backends.statuspage_register  # noqa: F401
    from pipewatch.backends import get_backend
    from pipewatch.backends.statuspage import StatuspageAlertBackend

    backend = get_backend(
        "statuspage",
        {
            "api_key": "k",
            "page_id": "p",
            "component_id": "c",
        },
    )
    assert isinstance(backend, StatuspageAlertBackend)
    assert backend._cfg.base_url == "https://api.statuspage.io/v1"
    assert backend._cfg.timeout == 10


def test_factory_passes_custom_config():
    import pipewatch.backends.statuspage_register  # noqa: F401
    from pipewatch.backends import get_backend

    backend = get_backend(
        "statuspage",
        {
            "api_key": "my-key",
            "page_id": "pg1",
            "component_id": "cmp1",
            "base_url": "https://custom.example.com/v1",
            "timeout": "30",
        },
    )
    assert backend._cfg.api_key == "my-key"
    assert backend._cfg.page_id == "pg1"
    assert backend._cfg.component_id == "cmp1"
    assert backend._cfg.base_url == "https://custom.example.com/v1"
    assert backend._cfg.timeout == 30
