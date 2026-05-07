"""Unit tests for GotifyAlertConfig priority_map defaults and overrides."""
from __future__ import annotations

import pytest

from pipewatch.backends.gotify import GotifyAlertBackend, GotifyAlertConfig


def _backend(priority_map=None):
    cfg = GotifyAlertConfig(
        url="https://gotify.example.com",
        token="t",
        **(dict(priority_map=priority_map) if priority_map is not None else {}),
    )
    return GotifyAlertBackend(cfg)


from pipewatch.alerting import AlertEvent


def _event(status):
    return AlertEvent(pipeline_id="p", status=status, reason="")


def test_default_priority_map_covers_standard_statuses():
    b = _backend()
    for status in ("critical", "warning", "ok", "unknown"):
        payload = b._build_payload(_event(status))
        assert isinstance(payload["priority"], int)


def test_custom_priority_map_overrides_all_levels():
    pm = {"critical": 10, "warning": 6, "ok": 0, "unknown": 2}
    b = _backend(priority_map=pm)
    assert b._build_payload(_event("critical"))["priority"] == 10
    assert b._build_payload(_event("warning"))["priority"] == 6
    assert b._build_payload(_event("ok"))["priority"] == 0
    assert b._build_payload(_event("unknown"))["priority"] == 2


def test_missing_status_in_map_falls_back_to_3():
    b = _backend(priority_map={"critical": 9})
    payload = b._build_payload(_event("warning"))
    assert payload["priority"] == 3


def test_reason_none_renders_na():
    b = _backend()
    event = AlertEvent(pipeline_id="p", status="ok", reason=None)
    payload = b._build_payload(event)
    assert "n/a" in payload["message"]


def test_trailing_slash_in_url_is_stripped():
    cfg = GotifyAlertConfig(url="https://gotify.example.com/", token="t")
    b = GotifyAlertBackend(cfg)
    from unittest.mock import MagicMock, patch

    resp = MagicMock()
    resp.status = 200
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=resp) as mock_open:
        b.send(_event("ok"))
    req = mock_open.call_args[0][0]
    assert req.full_url == "https://gotify.example.com/message"
