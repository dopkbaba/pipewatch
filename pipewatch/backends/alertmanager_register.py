"""Register the Alertmanager alert backend with pipewatch."""
from __future__ import annotations

from typing import Any


def _factory(cfg: dict[str, Any] | None = None) -> object:
    from pipewatch.backends.alertmanager import AlertmanagerAlertBackend, AlertmanagerConfig

    if cfg is None:
        return AlertmanagerAlertBackend()

    config = AlertmanagerConfig(
        url=cfg.get("url", "http://localhost:9093"),
        generator_url=cfg.get("generator_url", "http://pipewatch/"),
        labels=cfg.get("labels", {}),
        timeout=int(cfg.get("timeout", 10)),
    )
    return AlertmanagerAlertBackend(config)


try:
    from pipewatch.backends import register_backend

    register_backend("alertmanager", _factory)
except Exception:  # pragma: no cover
    pass
