"""Register the Statuspage.io alert backend with pipewatch."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.statuspage import (
        StatuspageAlertBackend,
        StatuspageAlertConfig,
    )

    cfg = StatuspageAlertConfig(
        api_key=config["api_key"],
        page_id=config["page_id"],
        component_id=config["component_id"],
        base_url=config.get(
            "base_url", "https://api.statuspage.io/v1"
        ),
        timeout=int(config.get("timeout", 10)),
    )
    return StatuspageAlertBackend(cfg)


register_backend("statuspage", _factory)
