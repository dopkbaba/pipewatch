"""Tests for the SQLite backend."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.backends.sqlite import SqliteBackend


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    """Create a minimal SQLite database with two pipeline rows."""
    path = tmp_path / "metrics.db"
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE pipeline_metrics (
            pipeline_id  TEXT    NOT NULL,
            last_run     TEXT,
            error_rate   REAL,
            row_count    INTEGER,
            PRIMARY KEY (pipeline_id)
        )
        """
    )
    conn.execute(
        "INSERT INTO pipeline_metrics VALUES (?, ?, ?, ?)",
        ("pipe_a", "2024-06-01T12:00:00+00:00", 0.01, 5000),
    )
    conn.execute(
        "INSERT INTO pipeline_metrics VALUES (?, ?, ?, ?)",
        ("pipe_b", None, None, None),
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture()
def backend(db_path: Path) -> SqliteBackend:
    return SqliteBackend(db_path=db_path)


# ---------------------------------------------------------------------------
# construction
# ---------------------------------------------------------------------------


def test_raises_when_db_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="not found"):
        SqliteBackend(db_path=tmp_path / "nonexistent.db")


# ---------------------------------------------------------------------------
# list_pipelines
# ---------------------------------------------------------------------------


def test_list_pipelines_returns_sorted_ids(backend: SqliteBackend) -> None:
    assert backend.list_pipelines() == ["pipe_a", "pipe_b"]


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------


def test_fetch_known_pipeline(backend: SqliteBackend) -> None:
    metrics = backend.fetch("pipe_a")
    assert metrics.pipeline_id == "pipe_a"
    assert metrics.error_rate == pytest.approx(0.01)
    assert metrics.row_count == 5000


def test_fetch_last_run_is_aware(backend: SqliteBackend) -> None:
    metrics = backend.fetch("pipe_a")
    assert metrics.last_run is not None
    assert metrics.last_run.tzinfo is not None


def test_fetch_last_run_naive_becomes_utc(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO pipeline_metrics VALUES (?, ?, ?, ?)",
        ("pipe_naive", "2024-06-01T08:00:00", 0.0, 100),
    )
    conn.commit()
    conn.close()

    backend = SqliteBackend(db_path=db_path)
    metrics = backend.fetch("pipe_naive")
    assert metrics.last_run is not None
    assert metrics.last_run.tzinfo == timezone.utc


def test_fetch_null_fields(backend: SqliteBackend) -> None:
    metrics = backend.fetch("pipe_b")
    assert metrics.last_run is None
    assert metrics.error_rate is None
    assert metrics.row_count is None


def test_fetch_unknown_pipeline_raises(backend: SqliteBackend) -> None:
    with pytest.raises(KeyError, match="pipe_unknown"):
        backend.fetch("pipe_unknown")
