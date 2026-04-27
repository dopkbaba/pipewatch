"""SMTP e-mail alert backend for pipewatch."""
from __future__ import annotations

import smtplib
from dataclasses import dataclass, field
from email.mime.text import MIMEText
from typing import List

from pipewatch.alerting import AlertEvent


@dataclass
class SMTPAlertConfig:
    host: str = "localhost"
    port: int = 587
    username: str = ""
    password: str = ""
    from_addr: str = "pipewatch@localhost"
    to_addrs: List[str] = field(default_factory=list)
    use_tls: bool = True


class SMTPAlertBackend:
    """Send pipeline alert notifications via SMTP e-mail."""

    def __init__(self, config: SMTPAlertConfig | None = None) -> None:
        self._cfg = config or SMTPAlertConfig()

    # ------------------------------------------------------------------
    def _build_subject(self, event: AlertEvent) -> str:
        return (
            f"[pipewatch] {event.status.upper()} "
            f"\u2013 pipeline '{event.pipeline_id}'"
        )

    def _build_body(self, event: AlertEvent) -> str:
        lines = [
            f"Pipeline : {event.pipeline_id}",
            f"Status   : {event.status.upper()}",
            f"Message  : {event.message}",
        ]
        if event.details:
            lines.append("Details  :")
            for k, v in event.details.items():
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)

    def send(self, event: AlertEvent) -> None:
        """Compose and dispatch an e-mail for *event*."""
        if not self._cfg.to_addrs:
            raise ValueError("SMTPAlertConfig.to_addrs must not be empty")

        msg = MIMEText(self._build_body(event))
        msg["Subject"] = self._build_subject(event)
        msg["From"] = self._cfg.from_addr
        msg["To"] = ", ".join(self._cfg.to_addrs)

        smtp_cls = smtplib.SMTP
        with smtp_cls(self._cfg.host, self._cfg.port) as conn:
            if self._cfg.use_tls:
                conn.starttls()
            if self._cfg.username:
                conn.login(self._cfg.username, self._cfg.password)
            conn.sendmail(
                self._cfg.from_addr,
                self._cfg.to_addrs,
                msg.as_string(),
            )
