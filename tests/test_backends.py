"""Tests for the pluggable backend system."""

from datetime import datetime, timezone

import pytest

from pipewatch.backends import (
    MemoryBackend,
    BackendError,
    PipelineMetrics,
    get_backend,
    register_backend,
)
from pipewatch.backends.base import BackendBase


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# PipelineMetrics
# ---------------------------------------------------------------------------

class TestPipelineMetrics:
    def test_defaults_are_none(self):
        m = PipelineMetrics(pipeline_id="p1")
        assert m.last_run is None
        assert m.row_count is None
        assert m.extra == {}

    def test_repr_contains_id(self):
        m = PipelineMetrics(pipeline_id="my_pipe", row_count=42)
        assert "my_pipe" in repr(m)
        assert "42" in repr(m)


# ---------------------------------------------------------------------------
# MemoryBackend
# ---------------------------------------------------------------------------

class TestMemoryBackend:
    def setup_method(self):
        self.backend = MemoryBackend()

    def test_register_and_fetch(self):
        self.backend.register("etl_orders", last_run=NOW, row_count=1000)
        m = self.backend.fetch("etl_orders")
        assert m.pipeline_id == "etl_orders"
        assert m.last_run == NOW
        assert m.row_count == 1000

    def test_fetch_unknown_raises(self):
        with pytest.raises(BackendError, match="not found"):
            self.backend.fetch("ghost_pipeline")

    def test_list_pipelines(self):
        self.backend.register("a")
        self.backend.register("b")
        ids = self.backend.list_pipelines()
        assert set(ids) == {"a", "b"}

    def test_clear_removes_all(self):
        self.backend.register("x")
        self.backend.clear()
        assert self.backend.list_pipelines() == []

    def test_extra_kwargs_stored(self):
        self.backend.register("p", custom_field="hello")
        m = self.backend.fetch("p")
        assert m.extra["custom_field"] == "hello"


# ---------------------------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------------------------

class TestBackendRegistry:
    def test_get_backend_memory(self):
        b = get_backend("memory")
        assert isinstance(b, MemoryBackend)

    def test_get_backend_unknown_raises(self):
        with pytest.raises(KeyError, match="unknown_backend"):
            get_backend("unknown_backend")

    def test_register_custom_backend(self):
        class DummyBackend(BackendBase):
            def fetch(self, pipeline_id):
                return PipelineMetrics(pipeline_id=pipeline_id)

            def list_pipelines(self):
                return []

        register_backend("dummy", DummyBackend)
        b = get_backend("dummy")
        assert isinstance(b, DummyBackend)

    def test_register_non_backend_raises(self):
        with pytest.raises(TypeError):
            register_backend("bad", object)  # type: ignore[arg-type]
