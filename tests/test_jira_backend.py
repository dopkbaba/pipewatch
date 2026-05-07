"""Tests for pipewatch.backends.jira."""
from __future__ import annotations

import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.jira import JiraAlertBackend, JiraAlertConfig
from pipewatch.health import HealthStatus


def _make_backend(**kwargs) -> JiraAlertBackend:
    cfg = JiraAlertConfig(
        base_url="https://example.atlassian.net",
        user_email="bot@example.com",
        api_token="secret",
        project_key="OPS",
        **kwargs,
    )
    return JiraAlertBackend(cfg)


def _make_event(
    pipeline_id: str = "pipe-1",
    status: str = HealthStatus.CRITICAL,
    message: str = "stale pipeline",
) -> AlertEvent:
    return AlertEvent(pipeline_id=pipeline_id, status=status, message=message)


@contextmanager
def _mock_urlopen(status: int = 201):
    fake_resp = MagicMock()
    fake_resp.status = status
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)
    with patch("pipewatch.backends.jira.urllib.request.urlopen", return_value=fake_resp) as m:
        yield m


class TestJiraPayload:
    def test_summary_contains_pipeline_id(self):
        backend = _make_backend()
        event = _make_event(pipeline_id="etl-prod")
        payload = backend._build_payload(event)
        assert "etl-prod" in payload["fields"]["summary"]

    def test_summary_contains_status_upper(self):
        backend = _make_backend()
        event = _make_event(status=HealthStatus.WARNING)
        payload = backend._build_payload(event)
        assert "WARNING" in payload["fields"]["summary"]

    def test_project_key_in_payload(self):
        backend = _make_backend(project_key="DATA")
        event = _make_event()
        payload = backend._build_payload(event)
        assert payload["fields"]["project"]["key"] == "DATA"

    def test_issue_type_default_is_bug(self):
        backend = _make_backend()
        event = _make_event()
        payload = backend._build_payload(event)
        assert payload["fields"]["issuetype"]["name"] == "Bug"

    def test_labels_included(self):
        backend = _make_backend(labels=["pipewatch", "critical"])
        event = _make_event()
        payload = backend._build_payload(event)
        assert "pipewatch" in payload["fields"]["labels"]
        assert "critical" in payload["fields"]["labels"]

    def test_description_contains_message(self):
        backend = _make_backend()
        event = _make_event(message="row count dropped")
        payload = backend._build_payload(event)
        assert "row count dropped" in payload["fields"]["description"]

    def test_send_posts_to_correct_url(self):
        backend = _make_backend()
        event = _make_event()
        with _mock_urlopen(201) as mock_open:
            backend.send(event)
        call_args = mock_open.call_args[0][0]
        assert "/rest/api/3/issue" in call_args.full_url

    def test_send_raises_on_unexpected_status(self):
        backend = _make_backend()
        event = _make_event()
        with _mock_urlopen(500):
            with pytest.raises(RuntimeError, match="500"):
                backend.send(event)

    def test_authorization_header_is_basic(self):
        backend = _make_backend()
        event = _make_event()
        with _mock_urlopen(201) as mock_open:
            backend.send(event)
        req = mock_open.call_args[0][0]
        assert req.get_header("Authorization").startswith("Basic ")
