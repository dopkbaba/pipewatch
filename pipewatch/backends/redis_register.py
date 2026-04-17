"""Auto-registration of the Redis backend with the backend registry."""

from __future__ import annotations

from typing import Any


def _factory(config: dict[str, Any]):
    """Create a RedisBackend from a config dict.

    Expected keys (all optional, fall back to redis defaults):
        host  – Redis host (default: localhost)
        port  – Redis port (default: 6379)
        db    – Redis database index (default: 0)
        password – Redis password (default: None)
        prefix – key prefix used when storing pipeline hashes (default: "pipewatch:")
    """
    from pipewatch.backends.redis import RedisBackend

    return RedisBackend(
        host=config.get("host", "localhost"),
        port=int(config.get("port", 6379)),
        db=int(config.get("db", 0)),
        password=config.get("password"),
        prefix=config.get("prefix", "pipewatch:"),
    )


# Register when this module is imported
from pipewatch.backends import register_backend  # noqa: E402

register_backend("redis", _factory)
