"""Sentry backend — reads pipeline health metrics from Sentry Issues API."""
from __future__ import annotations

import urllib.request
import urllib.error
import json
from datetime import datetime, timezone
from typing import Optional, List

from pipewatch.backends.base import BackendBase, PipelineMetrics


class SentryBackend(BackendBase):
    """Fetch pipeline metrics from a Sentry project via the Issues API."""

    def __init__(
        self,
        dsn: str,
        auth_token: str,
        org_slug: str,
        project_slug: str,
        timeout: int = 10,
    ) -> None:
        self._base_url = dsn.rstrip("/")
        self._token = auth_token
        self._org = org_slug
        self._project = project_slug
        self._timeout = timeout

    # ------------------------------------------------------------------
    def _get(self, path: str) -> dict:
        url = f"{self._base_url}{path}"
        req = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {self._token}"}
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                if resp.status != 200:
                    raise RuntimeError(
                        f"Sentry API returned {resp.status} for {url}"
                    )
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            raise RuntimeError(
                f"Sentry API error {exc.code} for {url}"
            ) from exc

    # ------------------------------------------------------------------
    def _parse_ts(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        dt = datetime.fromisoformat(value.rstrip("Z"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    # ------------------------------------------------------------------
    def list_pipelines(self) -> List[str]:
        path = f"/api/0/projects/{self._org}/{self._project}/issues/?query=pipeline"
        issues = self._get(path)
        ids: List[str] = []
        for issue in issues:
            tags = {t["key"]: t["value"] for t in issue.get("tags", [])}
            pid = tags.get("pipeline_id")
            if pid and pid not in ids:
                ids.append(pid)
        return sorted(ids)

    # ------------------------------------------------------------------
    def fetch(self, pipeline_id: str) -> PipelineMetrics:
        path = (
            f"/api/0/projects/{self._org}/{self._project}/issues/"
            f"?query=pipeline_id:{pipeline_id}"
        )
        issues = self._get(path)
        if not issues:
            return PipelineMetrics(pipeline_id=pipeline_id)

        latest = issues[0]
        tags = {t["key"]: t["value"] for t in latest.get("tags", [])}
        return PipelineMetrics(
            pipeline_id=pipeline_id,
            last_run=self._parse_ts(tags.get("last_run")),
            row_count=int(tags["row_count"]) if tags.get("row_count") else None,
            error_count=int(latest.get("count", 0)),
        )
