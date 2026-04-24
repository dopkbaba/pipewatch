"""Tests for the Prometheus Alertmanager alert backend."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.alertmanager import AlertmanagerAlertBackend, AlertmanagerConfig
from pipewatch.health import HealthStatus


def _make_backend(url="http://am:9093", labels=None, timeout=5):
    cfg = AlertmanagerConfig(
        url=url,
        generator_url="http://pipewatch/",
        labels=labels or {},
        timeout=timeout,
    )
    return AlertmanagerAlertBackend(cfg)


def _make_event(status=HealthStatus.CRITICAL, pipeline_id="pipe-1", message="stale"):
    return AlertEvent(pipeline_id=pipeline_id, status=status, message=message)


class TestAlertmanagerPayload:
    def test_labels_contain_pipeline_id(self):
        backend = _make_backend()
        event = _make_event()
        payload = backend._build_payload(event)
        assert payload[0]["labels"]["pipeline"] == "pipe-1"

    def test_labels_contain_severity_lower(self):
        backend = _make_backend()
        event = _make_event(status=HealthStatus.WARNING)
        payload = backend._build_payload(event)
        assert payload[0]["labels"]["severity"] == "warning"

    def test_extra_labels_merged(self):
        backend = _make_backend(labels={"env": "prod"})
        payload = backend._build_payload(_make_event())
        assert payload[0]["labels"]["env"] == "prod"

    def test_annotations_contain_summary(self):
        backend = _make_backend()
        event = _make_event()
        payload = backend._build_payload(event)
        assert "pipe-1" in payload[0]["annotations"]["summary"]

    def test_annotations_contain_description_when_message_set(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event(message="something broke"))
        assert payload[0]["annotations"]["description"] == "something broke"

    def test_no_description_when_message_empty(self):
        backend = _make_backend()
        event = AlertEvent(pipeline_id="p", status=HealthStatus.OK, message="")
        payload = backend._build_payload(event)
        assert "description" not in payload[0]["annotations"]

    def test_generator_url_present(self):
        backend = _make_backend()
        payload = backend._build_payload(_make_event())
        assert payload[0]["generatorURL"] == "http://pipewatch/"


class TestAlertmanagerSend:
    def _mock_urlopen(self, status=200):
        cm = MagicMock()
        cm.__enter__ = lambda s: s
        cm.__exit__ = MagicMock(return_value=False)
        cm.status = status
        return cm

    def test_posts_json_to_alertmanager(self):
        backend = _make_backend()
        event = _make_event()
        mock_resp = self._mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            backend.send(event)
            req = mock_open.call_args[0][0]
            body = json.loads(req.data)
            assert body[0]["labels"]["pipeline"] == "pipe-1"

    def test_raises_on_non_2xx(self):
        backend = _make_backend()
        mock_resp = self._mock_urlopen(500)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            with pytest.raises(RuntimeError, match="HTTP 500"):
                backend.send(_make_event())

    def test_url_has_correct_path(self):
        backend = _make_backend(url="http://am:9093")
        mock_resp = self._mock_urlopen(200)
        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            backend.send(_make_event())
            req = mock_open.call_args[0][0]
            assert req.full_url == "http://am:9093/api/v2/alerts"


def test_default_config_used_when_none_given():
    backend = AlertmanagerAlertBackend()
    assert backend._cfg.url == "http://localhost:9093"
    assert backend._cfg.timeout == 10
