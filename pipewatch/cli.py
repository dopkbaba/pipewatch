"""CLI entry point for pipewatch."""

import sys
import json
from datetime import datetime, timezone

import click

from pipewatch.backends import get_backend
from pipewatch.checker import HealthChecker, CheckerConfig
from pipewatch.alerting import AlertManager, AlertConfig
from pipewatch.health import HealthStatus


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


@click.group()
def cli():
    """pipewatch — monitor and alert on ETL pipeline health."""


@cli.command("check")
@click.option("--backend", default="memory", show_default=True, help="Backend name to use.")
@click.option("--pipeline", "pipeline_ids", multiple=True, help="Pipeline IDs to check (default: all).")
@click.option("--warn-after", default=3600, show_default=True, type=int, help="Seconds before warning on stale pipeline.")
@click.option("--crit-after", default=7200, show_default=True, type=int, help="Seconds before critical on stale pipeline.")
@click.option("--max-error-rate", default=0.1, show_default=True, type=float, help="Max acceptable error rate (0-1).")
@click.option("--output", type=click.Choice(["text", "json"]), default="text", show_default=True)
@click.option("--alert/--no-alert", default=False, help="Dispatch alerts for non-OK statuses.")
def check(
    backend: str,
    pipeline_ids: tuple,
    warn_after: int,
    crit_after: int,
    max_error_rate: float,
    output: str,
    alert: bool,
):
    """Check health of one or more pipelines."""
    try:
        backend_instance = get_backend(backend)
    except KeyError:
        click.echo(f"ERROR: Unknown backend '{backend}'", err=True)
        sys.exit(2)

    ids = list(pipeline_ids) if pipeline_ids else backend_instance.list_pipelines()
    if not ids:
        click.echo("No pipelines found.", err=True)
        sys.exit(0)

    config = CheckerConfig(
        warn_after_seconds=warn_after,
        crit_after_seconds=crit_after,
        max_error_rate=max_error_rate,
    )
    checker = HealthChecker(backend=backend_instance, config=config)

    alert_manager = AlertManager(config=AlertConfig()) if alert else None
    results = []
    exit_code = 0

    for pid in ids:
        health = checker.check(pid)
        results.append(health)
        if health.status in (HealthStatus.WARNING, HealthStatus.CRITICAL):
            exit_code = 1
        if alert_manager:
            alert_manager.process(health)

    if output == "json":
        click.echo(json.dumps([h.to_dict() for h in results], default=str, indent=2))
    else:
        for h in results:
            status_label = h.status.value.upper()
            click.echo(f"[{status_label}] {h.pipeline_id}: {h.message}")

    sys.exit(exit_code)


@cli.command("list")
@click.option("--backend", default="memory", show_default=True)
def list_pipelines(backend: str):
    """List all known pipelines in the backend."""
    try:
        backend_instance = get_backend(backend)
    except KeyError:
        click.echo(f"ERROR: Unknown backend '{backend}'", err=True)
        sys.exit(2)

    ids = backend_instance.list_pipelines()
    if not ids:
        click.echo("No pipelines registered.")
    for pid in ids:
        click.echo(pid)


if __name__ == "__main__":
    cli()
