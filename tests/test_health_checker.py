"""Tests for PipelineHealth evaluation and HealthChecker."""

from datetime import datetime, timedelta

import pytest

from pipewatch.checker import CheckerConfig, HealthChecker
from pipewatch.health import HealthStatus, PipelineHealth


def make_pipeline(name="test", minutes_ago=5, errors=0, duration=10.0):
    return PipelineHealth(
        name=name,
        last_run_at=datetime.utcnow() - timedelta(minutes=minutes_ago),
        last_run_duration_seconds=duration,
        records_processed=100,
        error_count=errors,
    )


class TestPipelineHealthEvaluate:
    def test_ok_when_all_thresholds_met(self):
        p = make_pipeline(minutes_ago=1, errors=0, duration=5.0)
        assert p.evaluate(max_duration_seconds=10.0, stale_after_seconds=300) == HealthStatus.OK

    def test_unknown_when_no_last_run(self):
        p = PipelineHealth(name="empty")
        assert p.evaluate() == HealthStatus.UNKNOWN

    def test_critical_on_stale_pipeline(self):
        p = make_pipeline(minutes_ago=60)
        assert p.evaluate(stale_after_seconds=600) == HealthStatus.CRITICAL

    def test_critical_on_errors(self):
        p = make_pipeline(errors=3)
        assert p.evaluate(max_error_count=0) == HealthStatus.CRITICAL

    def test_warning_on_slow_duration(self):
        p = make_pipeline(duration=120.0)
        assert p.evaluate(max_duration_seconds=60.0, stale_after_seconds=3600) == HealthStatus.WARNING

    def test_to_dict_contains_expected_keys(self):
        p = make_pipeline()
        d = p.to_dict()
        assert set(d.keys()) == {
            "name", "last_run_at", "last_run_duration_seconds",
            "records_processed", "error_count", "tags",
        }


class TestHealthChecker:
    def test_check_returns_ok_result(self):
        checker = HealthChecker(CheckerConfig(stale_after_seconds=3600))
        result = checker.check(make_pipeline())
        assert result.status == HealthStatus.OK
        assert result.is_healthy()

    def test_check_all_returns_one_result_per_pipeline(self):
        checker = HealthChecker()
        pipelines = [make_pipeline(f"pipe_{i}") for i in range(3)]
        results = checker.check_all(pipelines)
        assert len(results) == 3

    def test_message_contains_pipeline_name(self):
        checker = HealthChecker(CheckerConfig(max_error_count=0))
        result = checker.check(make_pipeline(name="my_etl", errors=2))
        assert "my_etl" in result.message
        assert "CRITICAL" in result.message

    def test_warning_message_includes_duration(self):
        config = CheckerConfig(max_duration_seconds=30.0, stale_after_seconds=3600)
        checker = HealthChecker(config)
        result = checker.check(make_pipeline(duration=90.0))
        assert result.status == HealthStatus.WARNING
        assert "90.0" in result.message
