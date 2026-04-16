"""Register the DynamoDB backend under the 'dynamodb' key."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.dynamodb import DynamoDBBackend

    return DynamoDBBackend(
        table_name=config["table_name"],
        region_name=config.get("region_name", "us-east-1"),
        endpoint_url=config.get("endpoint_url"),
    )


register_backend("dynamodb", _factory)
