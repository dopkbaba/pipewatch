"""Linear (linear.app) alert backend for pipewatch."""
from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent
from pipewatch.health import HealthStatus

_LINEAR_API = "https://api.linear.app/graphql"

_PRIORITY_MAP = {
    HealthStatus.CRITICAL: 1,   # Urgent
    HealthStatus.WARNING: 2,    # High
    HealthStatus.OK: 4,         # No priority
    HealthStatus.UNKNOWN: 3,    # Medium
}


@dataclass
class LinearAlertConfig:
    api_key: str
    team_id: str
    label_id: Optional[str] = None
    assignee_id: Optional[str] = None
    timeout: int = 10
    extra_headers: dict = field(default_factory=dict)


class LinearAlertBackend:
    """Creates Linear issues for pipeline health alerts."""

    def __init__(self, config: LinearAlertConfig) -> None:
        self._cfg = config

    def _build_payload(self, event: AlertEvent) -> dict:
        status = event.status.upper()
        title = f"[pipewatch] {event.pipeline_id} is {status}"
        body_lines = [
            f"**Pipeline:** `{event.pipeline_id}`",
            f"**Status:** {status}",
        ]
        if event.message:
            body_lines.append(f"**Details:** {event.message}")

        mutation = """
        mutation CreateIssue($input: IssueCreateInput!) {
          issueCreate(input: $input) {
            success
            issue { id identifier url }
          }
        }
        """
        variables: dict = {
            "input": {
                "teamId": self._cfg.team_id,
                "title": title,
                "description": "\n".join(body_lines),
                "priority": _PRIORITY_MAP.get(
                    HealthStatus(event.status.lower()), 3
                ),
            }
        }
        if self._cfg.label_id:
            variables["input"]["labelIds"] = [self._cfg.label_id]
        if self._cfg.assignee_id:
            variables["input"]["assigneeId"] = self._cfg.assignee_id

        return {"query": mutation, "variables": variables}

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        data = json.dumps(payload).encode()
        headers = {
            "Content-Type": "application/json",
            "Authorization": self._cfg.api_key,
            **self._cfg.extra_headers,
        }
        req = urllib.request.Request(_LINEAR_API, data=data, headers=headers)
        with urllib.request.urlopen(req, timeout=self._cfg.timeout) as resp:
            body = json.loads(resp.read())
        if not body.get("data", {}).get("issueCreate", {}).get("success"):
            raise RuntimeError(f"Linear issue creation failed: {body}")
