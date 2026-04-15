"""Health checker that evaluates a list of pipelines against configured thresholds."""

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.health import HealthStatus, PipelineHealth


@dataclass
class CheckResult:
    pipeline: PipelineHealth
    status: HealthStatus
    message: str

    def is_healthy(self) -> bool:
        return self.status == HealthStatus.OK


@dataclass
class CheckerConfig:
    max_error_count: int = 0
    max_duration_seconds: Optional[float] = None
    stale_after_seconds: Optional[float] = None


class HealthChecker:
    """Evaluates pipeline health snapshots against a shared config."""

    def __init__(self, config: Optional[CheckerConfig] = None):
        self.config = config or CheckerConfig()

    def check(self, pipeline: PipelineHealth) -> CheckResult:
        status = pipeline.evaluate(
            max_error_count=self.config.max_error_count,
            max_duration_seconds=self.config.max_duration_seconds,
            stale_after_seconds=self.config.stale_after_seconds,
        )
        message = self._build_message(pipeline, status)
        return CheckResult(pipeline=pipeline, status=status, message=message)

    def check_all(self, pipelines: List[PipelineHealth]) -> List[CheckResult]:
        return [self.check(p) for p in pipelines]

    def _build_message(self, pipeline: PipelineHealth, status: HealthStatus) -> str:
        if status == HealthStatus.OK:
            return f"[{pipeline.name}] OK"
        if status == HealthStatus.UNKNOWN:
            return f"[{pipeline.name}] No run data available"
        if status == HealthStatus.CRITICAL:
            if pipeline.error_count > self.config.max_error_count:
                return (
                    f"[{pipeline.name}] CRITICAL: {pipeline.error_count} errors "
                    f"(threshold: {self.config.max_error_count})"
                )
            return f"[{pipeline.name}] CRITICAL: pipeline data is stale"
        if status == HealthStatus.WARNING:
            return (
                f"[{pipeline.name}] WARNING: duration {pipeline.last_run_duration_seconds}s "
                f"exceeds {self.config.max_duration_seconds}s"
            )
        return f"[{pipeline.name}] {status.value}"
