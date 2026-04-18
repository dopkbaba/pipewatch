"""Register the CloudWatch backend with pipewatch's backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend
from pipewatch.backends.cloudwatch import CloudWatchBackend


def _factory(config: Dict[str, Any]) -> CloudWatchBackend:
    return CloudWatchBackend(
        namespace=config.get("namespace", "PipeWatch"),
        region=config.get("region", "us-east-1"),
    )


register_backend("cloudwatch", _factory)
