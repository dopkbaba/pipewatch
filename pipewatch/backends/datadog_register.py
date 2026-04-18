"""Register the Datadog backend with pipewatch's backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.datadog import DatadogBackend

    return DatadogBackend(
        api_key=config["api_key"],
        app_key=config["app_key"],
        metric_prefix=config.get("metric_prefix", "pipewatch"),
        host=config.get("host", "https://api.datadoghq.com/api/v1"),
        timeout=int(config.get("timeout", 10)),
    )


register_backend("datadog", _factory)
