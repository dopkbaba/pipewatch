"""Tests for the Datadog backend registration factory."""
from __future__ import annotations

import importlib
from unittest.mock import patch

import pytest

from pipewatch.backends import available_backends, get_backend


def _import_datadog_register():
    import pipewatch.backends.datadog_register  # noqa: F401  # ensure side-effects run
    importlib.reload(importlib.import_module("pipewatch.backends.datadog_register"))


def test_datadog_backend_is_registered():
    _import_datadog_register()
    assert "datadog" in available_backends()


def test_factory_creates_datadog_backend_with_defaults():
    from pipewatch.backends.datadog import DatadogBackend
    from pipewatch.backends.datadog_register import _factory

    backend = _factory({})

    assert isinstance(backend, DatadogBackend)
    assert backend._base_url == "https://api.datadoghq.com"
    assert backend._metric_prefix == "pipewatch"
    assert backend._timeout == 10


def test_factory_passes_custom_config():
    from pipewatch.backends.datadog import DatadogBackend
    from pipewatch.backends.datadog_register import _factory

    backend = _factory(
        {
            "base_url": "https://api.datadoghq.eu",
            "api_key": "abc123",
            "app_key": "xyz789",
            "metric_prefix": "myapp",
            "timeout": "30",
        }
    )

    assert isinstance(backend, DatadogBackend)
    assert backend._base_url == "https://api.datadoghq.eu"
    assert backend._api_key == "abc123"
    assert backend._app_key == "xyz789"
    assert backend._metric_prefix == "myapp"
    assert backend._timeout == 30


def test_factory_partial_config_uses_defaults_for_missing_keys():
    from pipewatch.backends.datadog_register import _factory

    backend = _factory({"api_key": "only-this"})

    assert backend._api_key == "only-this"
    assert backend._base_url == "https://api.datadoghq.com"
    assert backend._metric_prefix == "pipewatch"


def test_get_backend_returns_datadog_instance():
    _import_datadog_register()
    from pipewatch.backends.datadog import DatadogBackend

    factory = get_backend("datadog")
    instance = factory({})
    assert isinstance(instance, DatadogBackend)
