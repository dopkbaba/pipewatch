"""Register the SMTP alert backend with the pipewatch backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend
from pipewatch.backends.smtp import SMTPAlertBackend, SMTPAlertConfig


def _factory(cfg: Dict[str, Any]) -> SMTPAlertBackend:
    config = SMTPAlertConfig(
        host=cfg.get("host", "localhost"),
        port=int(cfg.get("port", 587)),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        from_addr=cfg.get("from_addr", "pipewatch@localhost"),
        to_addrs=cfg.get("to_addrs", []),
        use_tls=bool(cfg.get("use_tls", True)),
    )
    return SMTPAlertBackend(config)


register_backend("smtp_alert", _factory)
