"""Register the New Relic backend with pipewatch's backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.newrelic import NewRelicBackend

    return NewRelicBackend(
        account_id=config["account_id"],
        api_key=config["api_key"],
        table=config.get("table", "PipelineMetrics"),
        timeout=int(config.get("timeout", 10)),
    )


register_backend("newrelic", _factory)
