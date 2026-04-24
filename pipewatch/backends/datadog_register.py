"""Register the Datadog backend under the 'datadog' key."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend
from pipewatch.backends.datadog import DatadogBackend

_DEFAULTS: Dict[str, Any] = {
    "base_url": "https://api.datadoghq.com",
    "api_key": "",
    "app_key": "",
    "metric_prefix": "pipewatch",
    "timeout": 10,
}


def _factory(config: Dict[str, Any]) -> DatadogBackend:
    merged = {**_DEFAULTS, **config}
    return DatadogBackend(
        base_url=merged["base_url"],
        api_key=merged["api_key"],
        app_key=merged["app_key"],
        metric_prefix=merged["metric_prefix"],
        timeout=int(merged["timeout"]),
    )


register_backend("datadog", _factory)
