"""File-based backend that reads pipeline metrics from a JSON file."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.backends.base import BackendBase, PipelineMetrics


class FileBackend(BackendBase):
    """Read pipeline metrics from a local JSON file.

    Expected file format::

        [
            {
                "pipeline_id": "my_pipeline",
                "last_run": "2024-01-15T12:00:00+00:00",
                "row_count": 1000,
                "error_count": 0,
                "duration_seconds": 42.5
            }
        ]
    """

    name = "file"

    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._data: dict[str, PipelineMetrics] = {}
        self._load()

    def _load(self) -> None:
        """Load and parse the JSON metrics file."""
        if not self._path.exists():
            raise FileNotFoundError(f"Metrics file not found: {self._path}")

        with self._path.open() as fh:
            records = json.load(fh)

        if not isinstance(records, list):
            raise ValueError("Metrics file must contain a JSON array")

        self._data = {}
        for record in records:
            pipeline_id = record["pipeline_id"]
            last_run_raw = record.get("last_run")
            last_run: Optional[datetime] = None
            if last_run_raw is not None:
                last_run = datetime.fromisoformat(last_run_raw)
                if last_run.tzinfo is None:
                    last_run = last_run.replace(tzinfo=timezone.utc)

            self._data[pipeline_id] = PipelineMetrics(
                pipeline_id=pipeline_id,
                last_run=last_run,
                row_count=record.get("row_count"),
                error_count=record.get("error_count"),
                duration_seconds=record.get("duration_seconds"),
            )

    def fetch(self, pipeline_id: str) -> Optional[PipelineMetrics]:
        return self._data.get(pipeline_id)

    def list_pipelines(self) -> List[str]:
        return list(self._data.keys())

    def reload(self) -> None:
        """Re-read the file from disk."""
        self._load()
