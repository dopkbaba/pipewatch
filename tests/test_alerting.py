"""Tests for pipewatch.alerting module."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.alerting import AlertConfig, AlertEvent, AlertManager, log_handler
from pipewatch.health import HealthStatus, PipelineHealth


def make_health(
    name: str = "my_pipeline",
    status: HealthStatus = HealthStatus.CRITICAL,
    reason: str = "Too many errors",
) -> PipelineHealth:
    return PipelineHealth(
        pipeline_name=name,
        status=status,
        reason=reason,
        last_run=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        error_rate=0.5,
        row_count=100,
    )


class TestAlertManager:
    def test_dispatches_alert_on_critical(self):
        manager = AlertManager()
        handler = MagicMock()
        manager.register(handler)

        health = make_health(status=HealthStatus.CRITICAL)
        event = manager.evaluate_and_alert(health)

        assert event is not None
        assert event.status == HealthStatus.CRITICAL
        handler.assert_called_once_with(event)

    def test_dispatches_alert_on_warning(self):
        manager = AlertManager()
        handler = MagicMock()
        manager.register(handler)

        health = make_health(status=HealthStatus.WARNING, reason="Slightly elevated errors")
        event = manager.evaluate_and_alert(health)

        assert event is not None
        assert event.status == HealthStatus.WARNING
        handler.assert_called_once()

    def test_suppresses_ok_by_default(self):
        manager = AlertManager()
        handler = MagicMock()
        manager.register(handler)

        health = make_health(status=HealthStatus.OK, reason="All good")
        event = manager.evaluate_and_alert(health)

        assert event is None
        handler.assert_not_called()

    def test_custom_config_notifies_on_ok(self):
        config = AlertConfig(notify_on=[HealthStatus.OK], suppress_ok=False)
        manager = AlertManager(config=config)
        handler = MagicMock()
        manager.register(handler)

        health = make_health(status=HealthStatus.OK, reason="All good")
        event = manager.evaluate_and_alert(health)

        assert event is not None
        handler.assert_called_once()

    def test_multiple_handlers_all_called(self):
        manager = AlertManager()
        h1, h2 = MagicMock(), MagicMock()
        manager.register(h1)
        manager.register(h2)

        health = make_health(status=HealthStatus.CRITICAL)
        manager.evaluate_and_alert(health)

        h1.assert_called_once()
        h2.assert_called_once()

    def test_alert_event_str_format(self):
        event = AlertEvent(
            pipeline_name="etl_job",
            status=HealthStatus.CRITICAL,
            message="Pipeline stalled",
        )
        assert str(event) == "[CRITICAL] etl_job: Pipeline stalled"

    def test_log_handler_prints(self, capsys):
        event = AlertEvent(
            pipeline_name="etl_job",
            status=HealthStatus.WARNING,
            message="High error rate",
        )
        log_handler(event)
        captured = capsys.readouterr()
        assert "[WARNING]" in captured.out
        assert "etl_job" in captured.out
