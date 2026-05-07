"""Jira alert backend — creates issues for pipeline health events."""
from __future__ import annotations

import json
import urllib.request
import urllib.error
import base64
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerting import AlertEvent


@dataclass
class JiraAlertConfig:
    base_url: str = "https://your-domain.atlassian.net"
    user_email: str = ""
    api_token: str = ""
    project_key: str = "OPS"
    issue_type: str = "Bug"
    labels: list = field(default_factory=lambda: ["pipewatch"])


class JiraAlertBackend:
    """Creates a Jira issue when a pipeline alert fires."""

    def __init__(self, config: JiraAlertConfig) -> None:
        self._cfg = config
        _creds = f"{config.user_email}:{config.api_token}"
        self._auth = base64.b64encode(_creds.encode()).decode()

    def _build_payload(self, event: AlertEvent) -> dict:
        summary = (
            f"[pipewatch] {event.pipeline_id} is {event.status.upper()}"
        )
        description = (
            f"Pipeline *{event.pipeline_id}* reported status *{event.status.upper()}*.\n\n"
            f"Message: {event.message or 'n/a'}"
        )
        return {
            "fields": {
                "project": {"key": self._cfg.project_key},
                "summary": summary,
                "description": description,
                "issuetype": {"name": self._cfg.issue_type},
                "labels": list(self._cfg.labels),
            }
        }

    def send(self, event: AlertEvent) -> None:
        payload = self._build_payload(event)
        url = f"{self._cfg.base_url.rstrip('/')}/rest/api/3/issue"
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Authorization": f"Basic {self._auth}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:  # noqa: S310
            if resp.status not in (200, 201):
                raise RuntimeError(
                    f"Jira API returned unexpected status {resp.status}"
                )
