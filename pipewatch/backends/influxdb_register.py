"""Register the InfluxDB backend with the pipewatch backend registry."""
from __future__ import annotations

from typing import Any, Dict


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.influxdb import InfluxDBBackend

    return InfluxDBBackend(
        url=config.get("url", "http://localhost:8086"),
        token=config.get("token", ""),
        org=config.get("org", "pipewatch"),
        bucket=config.get("bucket", "pipelines"),
        measurement=config.get("measurement", "pipeline_health"),
    )


def _register():
    from pipewatch.backends import register_backend

    register_backend("influxdb", _factory)


_register()
