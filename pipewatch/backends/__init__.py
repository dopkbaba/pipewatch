"""Pluggable backend registry for pipewatch metric sources."""

from pipewatch.backends.base import BackendBase, BackendError, PipelineMetrics
from pipewatch.backends.memory import MemoryBackend

_REGISTRY: dict[str, type[BackendBase]] = {
    "memory": MemoryBackend,
}


def register_backend(name: str, cls: type[BackendBase]) -> None:
    """Register a custom backend under *name* for use with get_backend()."""
    if not issubclass(cls, BackendBase):
        raise TypeError(f"{cls} must subclass BackendBase.")
    _REGISTRY[name] = cls


def get_backend(name: str, **kwargs) -> BackendBase:
    """Instantiate and return a registered backend by name.

    Args:
        name: The registered backend identifier (e.g. ``"memory"``).
        **kwargs: Keyword arguments forwarded to the backend constructor.

    Raises:
        KeyError: If *name* is not a registered backend.
    """
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY))
        raise KeyError(
            f"Unknown backend {name!r}. Available backends: {available}"
        )
    return _REGISTRY[name](**kwargs)


__all__ = [
    "BackendBase",
    "BackendError",
    "PipelineMetrics",
    "MemoryBackend",
    "register_backend",
    "get_backend",
]
