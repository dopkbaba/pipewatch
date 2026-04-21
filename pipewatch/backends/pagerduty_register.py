"""Register the PagerDuty alert backend with pipewatch."""
from __future__ import annotations

from typing import Any

from pipewatch.backends import register_backend
from pipewatch.backends.pagerduty import PagerDutyAlertBackend, PagerDutyAlertConfig


def _factory(cfg: dict[str, Any]) -> PagerDutyAlertBackend:
    """Construct a PagerDutyAlertBackend from a config dict.

    Required keys:
        integration_key (str): PagerDuty Events API v2 integration key.

    Optional keys:
        source (str): Event source label (default: 'pipewatch').
        timeout (int): HTTP timeout in seconds (default: 10).
        severity_map (dict): Override status -> PD severity mapping.
    """
    if "integration_key" not in cfg:
        raise ValueError("pagerduty backend requires 'integration_key'")

    kwargs: dict[str, Any] = {"integration_key": cfg["integration_key"]}
    if "source" in cfg:
        kwargs["source"] = cfg["source"]
    if "timeout" in cfg:
        kwargs["timeout"] = int(cfg["timeout"])
    if "severity_map" in cfg:
        kwargs["severity_map"] = cfg["severity_map"]

    return PagerDutyAlertBackend(PagerDutyAlertConfig(**kwargs))


register_backend("pagerduty", _factory)
