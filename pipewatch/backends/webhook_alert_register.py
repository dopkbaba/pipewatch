"""Register the generic webhook alert backend with pipewatch."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend
from pipewatch.backends.webhook_alert import WebhookAlertBackend, WebhookAlertConfig


def _factory(cfg: Dict[str, Any]) -> WebhookAlertBackend:
    """Construct a :class:`WebhookAlertBackend` from a config dict.

    Required keys
    -------------
    ``url``  – destination endpoint.

    Optional keys
    -------------
    ``method``, ``headers``, ``timeout``, ``extra``.
    """
    if "url" not in cfg:
        raise ValueError("webhook_alert backend requires 'url' in config")

    config = WebhookAlertConfig(
        url=cfg["url"],
        method=cfg.get("method", "POST"),
        headers=cfg.get("headers", {}),
        timeout=int(cfg.get("timeout", 10)),
        extra=cfg.get("extra", {}),
    )
    return WebhookAlertBackend(config)


register_backend("webhook_alert", _factory)
