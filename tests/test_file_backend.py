"""Tests for the file-based pipeline metrics backend."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.backends.file import FileBackend


_SAMPLE = [
    {
        "pipeline_id": "orders",
        "last_run": "2024-03-01T08:00:00+00:00",
        "row_count": 5000,
        "error_count": 2,
        "duration_seconds": 90.0,
    },
    {
        "pipeline_id": "inventory",
        "last_run": None,
        "row_count": None,
        "error_count": None,
        "duration_seconds": None,
    },
]


@pytest.fixture()
def metrics_file(tmp_path: Path) -> Path:
    fpath = tmp_path / "metrics.json"
    fpath.write_text(json.dumps(_SAMPLE))
    return fpath


class TestFileBackend:
    def test_list_pipelines(self, metrics_file: Path) -> None:
        backend = FileBackend(str(metrics_file))
        assert set(backend.list_pipelines()) == {"orders", "inventory"}

    def test_fetch_known_pipeline(self, metrics_file: Path) -> None:
        backend = FileBackend(str(metrics_file))
        metrics = backend.fetch("orders")
        assert metrics is not None
        assert metrics.pipeline_id == "orders"
        assert metrics.row_count == 5000
        assert metrics.error_count == 2
        assert metrics.duration_seconds == 90.0

    def test_fetch_last_run_is_aware(self, metrics_file: Path) -> None:
        backend = FileBackend(str(metrics_file))
        metrics = backend.fetch("orders")
        assert metrics is not None
        assert metrics.last_run is not None
        assert metrics.last_run.tzinfo is not None

    def test_fetch_pipeline_with_nulls(self, metrics_file: Path) -> None:
        backend = FileBackend(str(metrics_file))
        metrics = backend.fetch("inventory")
        assert metrics is not None
        assert metrics.last_run is None
        assert metrics.row_count is None

    def test_fetch_unknown_returns_none(self, metrics_file: Path) -> None:
        backend = FileBackend(str(metrics_file))
        assert backend.fetch("nonexistent") is None

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            FileBackend(str(tmp_path / "missing.json"))

    def test_invalid_format_raises(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.json"
        bad.write_text(json.dumps({"not": "a list"}))
        with pytest.raises(ValueError, match="JSON array"):
            FileBackend(str(bad))

    def test_reload_picks_up_changes(self, metrics_file: Path) -> None:
        backend = FileBackend(str(metrics_file))
        assert backend.fetch("orders") is not None

        new_data = [{"pipeline_id": "fresh", "last_run": None}]
        metrics_file.write_text(json.dumps(new_data))
        backend.reload()

        assert backend.fetch("orders") is None
        assert backend.fetch("fresh") is not None

    def test_naive_datetime_gets_utc(self, tmp_path: Path) -> None:
        data = [{"pipeline_id": "p", "last_run": "2024-01-01T00:00:00"}]
        fpath = tmp_path / "naive.json"
        fpath.write_text(json.dumps(data))
        backend = FileBackend(str(fpath))
        metrics = backend.fetch("p")
        assert metrics is not None
        assert metrics.last_run is not None
        assert metrics.last_run.tzinfo == timezone.utc
