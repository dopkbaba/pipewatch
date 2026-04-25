"""Register the MSTeamsWebhook alert backend with pipewatch."""
from __future__ import annotations

from pipewatch.backends import register_backend


def _factory(cfg: dict):
    from pipewatch.backends.msteams_webhook import (
        MSTeamsWebhookAlertBackend,
        MSTeamsWebhookConfig,
    )

    config = MSTeamsWebhookConfig(
        webhook_url=cfg["webhook_url"],
        mention_email=cfg.get("mention_email"),
        timeout=int(cfg.get("timeout", 10)),
        title_prefix=cfg.get("title_prefix", "[pipewatch]"),
    )
    return MSTeamsWebhookAlertBackend(config)


register_backend("msteams_webhook", _factory)
