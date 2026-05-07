"""Register the Gotify alert backend with pipewatch's backend registry."""
from __future__ import annotations

from pipewatch.backends import register_backend


def _factory(cfg: dict):
    from pipewatch.backends.gotify import GotifyAlertBackend, GotifyAlertConfig

    priority_map = cfg.get(
        "priority_map",
        {"critical": 9, "warning": 5, "ok": 1, "unknown": 3},
    )
    config = GotifyAlertConfig(
        url=cfg["url"],
        token=cfg["token"],
        priority_map=priority_map,
        timeout=int(cfg.get("timeout", 10)),
    )
    return GotifyAlertBackend(config)


register_backend("gotify", _factory)
