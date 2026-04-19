"""Register the Kinesis backend with pipewatch's backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.kinesis import KinesisBackend

    return KinesisBackend(
        stream_name=config["stream_name"],
        region_name=config.get("region_name", "us-east-1"),
        shard_iterator_type=config.get("shard_iterator_type", "LATEST"),
        endpoint_url=config.get("endpoint_url"),
    )


register_backend("kinesis", _factory)
