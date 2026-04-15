"""Backend registry for pipewatch."""

from __future__ import annotations

from typing import Dict, Type

from pipewatch.backends.base import BackendBase

_REGISTRY: Dict[str, Type[BackendBase]] = {}


def register_backend(name: str, cls: Type[BackendBase]) -> None:
    """Register a backend class under *name*."""
    _REGISTRY[name] = cls


def get_backend(name: str, **kwargs: object) -> BackendBase:
    """Instantiate and return a registered backend by name.

    Parameters
    ----------
    name:
        The backend identifier (e.g. ``"memory"``, ``"file"``)
    **kwargs:
        Constructor arguments forwarded to the backend class.

    Raises
    ------
    KeyError
        If *name* has not been registered.
    """
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise KeyError(
            f"Unknown backend {name!r}. Available backends: {available}"
        )
    return _REGISTRY[name](**kwargs)  # type: ignore[arg-type]


def available_backends() -> list[str]:
    """Return a sorted list of registered backend names."""
    return sorted(_REGISTRY.keys())


# Register built-in backends
from pipewatch.backends.memory import MemoryBackend  # noqa: E402
from pipewatch.backends.file import FileBackend  # noqa: E402

register_backend("memory", MemoryBackend)
register_backend("file", FileBackend)
