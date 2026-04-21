"""Register the VictoriaMetrics backend with the pipewatch backend registry."""
from __future__ import annotations

from typing import Any, Dict


def _factory(config: Dict[str, Any]):
    from pipewatch.backends.victoriametrics import VictoriaMetricsBackend

    return VictoriaMetricsBackend(
        base_url=config.get("base_url", "http://localhost:8428"),
        timeout=int(config.get("timeout", 10)),
    )


try:
    from pipewatch.backends import register_backend

    register_backend("victoriametrics", _factory)
except Exception:  # pragma: no cover
    pass
