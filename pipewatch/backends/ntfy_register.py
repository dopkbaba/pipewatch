"""Register the ntfy alert backend with pipewatch."""
from __future__ import annotations

from typing import Any, Dict


def _factory(cfg: Dict[str, Any] | None = None):
    from pipewatch.backends.ntfy import NtfyAlertBackend, NtfyAlertConfig

    cfg = cfg or {}
    priority_map = cfg.get("priority_map", None)
    config = NtfyAlertConfig(
        server=cfg.get("server", "https://ntfy.sh"),
        topic=cfg.get("topic", "pipewatch"),
        timeout=int(cfg.get("timeout", 10)),
        priority_map=priority_map if priority_map is not None else {
            "critical": 5,
            "warning": 3,
            "ok": 1,
            "unknown": 2,
        },
    )
    return NtfyAlertBackend(config)


try:
    from pipewatch.backends import register_backend
    register_backend("ntfy", _factory)
except Exception:  # pragma: no cover
    pass
