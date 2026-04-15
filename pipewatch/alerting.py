"""Alerting module for pipewatch — dispatches alerts based on pipeline health status."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.health import HealthStatus, PipelineHealth


@dataclass
class AlertEvent:
    """Represents a dispatched alert event."""

    pipeline_name: str
    status: HealthStatus
    message: str
    details: Dict[str, object] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"[{self.status.value.upper()}] {self.pipeline_name}: {self.message}"


AlertHandler = Callable[[AlertEvent], None]


@dataclass
class AlertConfig:
    """Configuration for the alerting system."""

    notify_on: List[HealthStatus] = field(
        default_factory=lambda: [HealthStatus.CRITICAL, HealthStatus.WARNING]
    )
    suppress_ok: bool = True


class AlertManager:
    """Manages alert handlers and dispatches alerts based on pipeline health."""

    def __init__(self, config: Optional[AlertConfig] = None) -> None:
        self.config = config or AlertConfig()
        self._handlers: List[AlertHandler] = []

    def register(self, handler: AlertHandler) -> None:
        """Register a new alert handler."""
        self._handlers.append(handler)

    def evaluate_and_alert(self, health: PipelineHealth) -> Optional[AlertEvent]:
        """Evaluate pipeline health and dispatch an alert if warranted."""
        if health.status not in self.config.notify_on:
            return None

        event = AlertEvent(
            pipeline_name=health.pipeline_name,
            status=health.status,
            message=health.reason or f"Pipeline entered {health.status.value} state.",
            details={
                "last_run": health.last_run.isoformat() if health.last_run else None,
                "error_rate": health.error_rate,
                "row_count": health.row_count,
            },
        )

        for handler in self._handlers:
            handler(event)

        return event


def log_handler(event: AlertEvent) -> None:
    """Built-in handler that prints alerts to stdout."""
    print(str(event))
