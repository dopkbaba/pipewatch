"""Tests for the pipewatch CLI."""

from datetime import datetime, timezone, timedelta

import pytest
from click.testing import CliRunner

from pipewatch.cli import cli
from pipewatch.backends import register_backend
from pipewatch.backends.memory import MemoryBackend
from pipewatch.backends.base import PipelineMetrics


@pytest.fixture(autouse=True)
def _fresh_backend():
    """Register a clean memory backend before each test."""
    backend = MemoryBackend()
    register_backend("memory", backend)
    yield backend


def _recent() -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(minutes=5)


def _stale(hours: int = 3) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(hours=hours)


class TestCheckCommand:
    def test_ok_pipeline_exits_zero(self, _fresh_backend):
        _fresh_backend.register(
            "pipe-ok",
            PipelineMetrics(pipeline_id="pipe-ok", last_run=_recent(), error_rate=0.0, rows_processed=100),
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["check", "--pipeline", "pipe-ok"])
        assert result.exit_code == 0
        assert "[OK]" in result.output

    def test_critical_pipeline_exits_one(self, _fresh_backend):
        _fresh_backend.register(
            "pipe-stale",
            PipelineMetrics(pipeline_id="pipe-stale", last_run=_stale(5), error_rate=0.0, rows_processed=10),
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["check", "--pipeline", "pipe-stale"])
        assert result.exit_code == 1
        assert "[CRITICAL]" in result.output

    def test_json_output_is_valid(self, _fresh_backend):
        import json
        _fresh_backend.register(
            "pipe-json",
            PipelineMetrics(pipeline_id="pipe-json", last_run=_recent(), error_rate=0.0, rows_processed=50),
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["check", "--pipeline", "pipe-json", "--output", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert data[0]["pipeline_id"] == "pipe-json"

    def test_unknown_backend_exits_two(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["check", "--backend", "nonexistent", "--pipeline", "x"])
        assert result.exit_code == 2
        assert "Unknown backend" in result.output

    def test_checks_all_pipelines_when_none_specified(self, _fresh_backend):
        for pid in ("a", "b", "c"):
            _fresh_backend.register(
                pid,
                PipelineMetrics(pipeline_id=pid, last_run=_recent(), error_rate=0.0, rows_processed=1),
            )
        runner = CliRunner()
        result = runner.invoke(cli, ["check"])
        assert result.exit_code == 0
        assert "[OK] a" in result.output
        assert "[OK] b" in result.output
        assert "[OK] c" in result.output


class TestListCommand:
    def test_lists_registered_pipelines(self, _fresh_backend):
        _fresh_backend.register(
            "pipe-x",
            PipelineMetrics(pipeline_id="pipe-x", last_run=_recent()),
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["list"])
        assert result.exit_code == 0
        assert "pipe-x" in result.output

    def test_empty_backend_message(self, _fresh_backend):
        runner = CliRunner()
        result = runner.invoke(cli, ["list"])
        assert result.exit_code == 0
        assert "No pipelines" in result.output
