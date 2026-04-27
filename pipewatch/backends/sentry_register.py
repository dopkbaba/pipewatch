"""Register the Sentry backend with pipewatch's pluggable backend registry."""
from __future__ import annotations

from typing import Any, Dict


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.sentry import SentryBackend  # noqa: PLC0415

    return SentryBackend(
        dsn=config.get("dsn", "https://sentry.io"),
        auth_token=config["auth_token"],
        org_slug=config.get("org_slug", ""),
        project_slug=config.get("project_slug", ""),
        timeout=int(config.get("timeout", 10)),
    )


try:
    from pipewatch.backends import register_backend  # noqa: PLC0415

    register_backend("sentry", _factory)
except Exception:  # pragma: no cover
    pass
