"""Unit tests for SignalRBackend._parse_ts edge cases."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.backends.signalr import SignalRBackend


@pytest.fixture()
def backend():
    return SignalRBackend()


def test_returns_none_for_none(backend):
    assert backend._parse_ts(None) is None


def test_returns_none_for_empty_string(backend):
    assert backend._parse_ts("") is None


def test_aware_timestamp_preserved(backend):
    ts = "2024-06-01T10:00:00+02:00"
    result = backend._parse_ts(ts)
    assert result is not None
    assert result.tzinfo is not None
    # Offset should be +02:00, not coerced to UTC
    assert result.utcoffset().total_seconds() == 7200


def test_naive_timestamp_becomes_utc(backend):
    ts = "2024-06-01T10:00:00"
    result = backend._parse_ts(ts)
    assert result is not None
    assert result.tzinfo == timezone.utc
    assert result == datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)


def test_date_only_string_raises(backend):
    """ISO date-only strings are not valid pipeline timestamps."""
    with pytest.raises(ValueError):
        backend._parse_ts("2024-06-01")
