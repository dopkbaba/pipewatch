"""Auto-register the Prometheus backend with the pipewatch backend registry.

Import this module (or include it in your pipewatch config) to make
``prometheus`` available as a backend name::

    import pipewatch.backends.prometheus_register  # noqa: F401

Or via the CLI with ``--backend prometheus``.
"""
from __future__ import annotations

from pipewatch.backends import register_backend
from pipewatch.backends.prometheus import PrometheusBackend


def _factory(config: dict) -> PrometheusBackend:
    """Create a :class:`PrometheusBackend` from a config dict.

    Expected keys:
      - ``url`` (required): base URL of the Prometheus-compatible endpoint.
      - ``timeout`` (optional, default 10): request timeout in seconds.
    """
    url = config.get("url")
    if not url:
        raise ValueError(
            "PrometheusBackend requires a 'url' entry in its config block."
        )
    timeout = int(config.get("timeout", 10))
    return PrometheusBackend(base_url=url, timeout=timeout)


register_backend("prometheus", _factory)

__all__ = ["PrometheusBackend", "_factory"]
