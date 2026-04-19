"""Register the SQLite backend with the pipewatch backend registry."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pipewatch.backends import register_backend
from pipewatch.backends.sqlite import SqliteBackend


def _factory(config: dict[str, Any]) -> SqliteBackend:
    """Construct a :class:`SqliteBackend` from a config mapping.

    Expected keys:
        ``db_path`` (str | Path): path to the ``.db`` file.

    Example config fragment::

        backend:
          type: sqlite
          db_path: /var/lib/pipewatch/metrics.db
    """
    db_path = config.get("db_path")
    if not db_path:
        raise ValueError("SqliteBackend requires 'db_path' in config")

    db_path = Path(db_path)

    if db_path.suffix != ".db":
        raise ValueError(
            f"SqliteBackend 'db_path' should have a '.db' extension, got: '{db_path.suffix}'"
        )

    if not db_path.parent.exists():
        raise ValueError(
            f"SqliteBackend 'db_path' parent directory does not exist: '{db_path.parent}'"
        )

    return SqliteBackend(db_path=db_path)


register_backend("sqlite", _factory)
