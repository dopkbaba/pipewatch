"""Backend registry for pipewatch.

Built-in backends
-----------------
- ``memory``  – in-process store, useful for testing
- ``file``    – reads metrics from a local JSON file
- ``http``    – fetches metrics from a remote JSON REST endpoint
"""

from __future__ import annotations

from typing import Dict, List, Type

from pipewatch.backends.base import BackendBase

_REGISTRY: Dict[str, Type[BackendBase]] = {}


def register_backend(name: str, cls: Type[BackendBase]) -> None:
    """Register a backend class under *name*."""
    _REGISTRY[name] = cls


def get_backend(name: str) -> Type[BackendBase]:
    """Return the backend class registered under *name*.

    Raises
    ------
    KeyError
        If no backend with that name has been registered.
    """
    try:
        return _REGISTRY[name]
    except KeyError:
        available = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise KeyError(f"Unknown backend {name!r}. Available: {available}") from None


def available_backends() -> List[str]:
    """Return a sorted list of registered backend names."""
    return sorted(_REGISTRY)


# ---------------------------------------------------------------------------
# Register built-in backends
# ---------------------------------------------------------------------------

from pipewatch.backends.memory import MemoryBackend  # noqa: E402
from pipewatch.backends.file import FileBackend  # noqa: E402

register_backend("memory", MemoryBackend)
register_backend("file", FileBackend)

try:
    from pipewatch.backends.http import HttpBackend  # noqa: E402
    register_backend("http", HttpBackend)
except ImportError:  # requests not installed
    pass
