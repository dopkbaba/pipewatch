"""Register BigQuery backend with pipewatch backend registry."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.bigquery import BigQueryBackend

    return BigQueryBackend(
        project=config["project"],
        dataset=config["dataset"],
        table=config.get("table", "pipeline_metrics"),
    )


register_backend("bigquery", _factory)
