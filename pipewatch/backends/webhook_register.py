"""Register the WebhookBackend under the name ``'webhook'``."""

from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend
from pipewatch.backends.webhook import WebhookBackend


def _factory(config: Dict[str, Any]) -> WebhookBackend:
    """Create a :class:`WebhookBackend` from a config dictionary.

    Required keys
    -------------
    ``base_url`` : str
        Root URL of the webhook service (e.g. ``"https://hooks.example.com"``).

    Optional keys
    -------------
    ``timeout`` : int  (default 10)
    ``token``   : str  (default None)
    """
    if "base_url" not in config:
        raise ValueError("WebhookBackend requires 'base_url' in config")
    return WebhookBackend(
        base_url=config["base_url"],
        timeout=int(config.get("timeout", 10)),
        token=config.get("token"),
    )


register_backend("webhook", _factory)
