"""Unit tests for the Statuspage status mapping."""
from __future__ import annotations

import pytest

from pipewatch.backends.statuspage import _STATUS_MAP, StatuspageAlertBackend, StatuspageAlertConfig


@pytest.mark.parametrize(
    "input_status,expected",
    [
        ("ok", "operational"),
        ("warning", "degraded_performance"),
        ("critical", "major_outage"),
        ("unknown", "under_maintenance"),
    ],
)
def test_status_map_values(input_status, expected):
    assert _STATUS_MAP[input_status] == expected


def test_status_map_covers_all_standard_statuses():
    expected_keys = {"ok", "warning", "critical", "unknown"}
    assert set(_STATUS_MAP.keys()) == expected_keys


def test_unknown_status_falls_back_to_under_maintenance():
    cfg = StatuspageAlertConfig(
        api_key="k", page_id="p", component_id="c"
    )
    backend = StatuspageAlertBackend(cfg)
    from pipewatch.alerting import AlertEvent
    event = AlertEvent(pipeline_id="x", status="bogus", message="")
    payload = backend._build_payload(event)
    assert payload["component"]["status"] == "under_maintenance"
