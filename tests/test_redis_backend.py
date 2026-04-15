"""Tests for RedisBackend using a fake redis client."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.redis import RedisBackend


class FakeRedis:
    """Minimal in-memory stub that mimics the redis.Redis API used by RedisBackend."""

    def __init__(self, store: Dict[str, Dict[str, str]]) -> None:
        self._store = store

    def keys(self, pattern: str) -> List[str]:
        prefix = pattern.rstrip("*")
        return [k for k in self._store if k.startswith(prefix)]

    def hgetall(self, key: str) -> Dict[str, str]:
        return self._store.get(key, {})


def _make_backend(store: Dict[str, Dict[str, str]], prefix: str = "pipewatch:") -> RedisBackend:
    backend = object.__new__(RedisBackend)
    backend._redis = FakeRedis(store)
    backend._prefix = prefix
    return backend


class TestRedisBackendListPipelines:
    def test_returns_sorted_ids(self):
        store = {
            "pipewatch:orders": {},
            "pipewatch:users": {},
            "pipewatch:events": {},
        }
        backend = _make_backend(store)
        assert backend.list_pipelines() == ["events", "orders", "users"]

    def test_returns_empty_when_no_keys(self):
        backend = _make_backend({})
        assert backend.list_pipelines() == []

    def test_ignores_keys_with_different_prefix(self):
        store = {
            "other:orders": {},
            "pipewatch:users": {},
        }
        backend = _make_backend(store)
        assert backend.list_pipelines() == ["users"]


class TestRedisBackendFetch:
    def test_returns_empty_metrics_for_missing_key(self):
        backend = _make_backend({})
        m = backend.fetch("missing")
        assert m.pipeline_id == "missing"
        assert m.last_run is None
        assert m.error_rate is None
        assert m.rows_processed is None

    def test_parses_aware_datetime(self):
        store = {
            "pipewatch:orders": {
                "last_run": "2024-03-15T10:00:00+00:00",
                "error_rate": "0.02",
                "rows_processed": "5000",
            }
        }
        backend = _make_backend(store)
        m = backend.fetch("orders")
        assert m.last_run == datetime(2024, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
        assert m.error_rate == pytest.approx(0.02)
        assert m.rows_processed == 5000

    def test_makes_naive_datetime_utc_aware(self):
        store = {"pipewatch:pipe": {"last_run": "2024-01-01T00:00:00"}}
        backend = _make_backend(store)
        m = backend.fetch("pipe")
        assert m.last_run is not None
        assert m.last_run.tzinfo == timezone.utc

    def test_tolerates_invalid_numeric_fields(self):
        store = {
            "pipewatch:bad": {
                "error_rate": "n/a",
                "rows_processed": "unknown",
            }
        }
        backend = _make_backend(store)
        m = backend.fetch("bad")
        assert m.error_rate is None
        assert m.rows_processed is None

    def test_import_error_raised_without_redis_package(self):
        with patch.dict("sys.modules", {"redis": None}):
            with pytest.raises(ImportError, match="redis-py"):
                RedisBackend()
