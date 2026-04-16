"""Register the MongoDB backend under the 'mongodb' key."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.mongodb import MongoDBBackend

    return MongoDBBackend(
        uri=config.get("uri", "mongodb://localhost:27017"),
        database=config.get("database", "pipewatch"),
        collection=config.get("collection", "pipeline_metrics"),
    )


register_backend("mongodb", _factory)
