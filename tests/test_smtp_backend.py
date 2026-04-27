"""Tests for the SMTP alert backend."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerting import AlertEvent
from pipewatch.backends.smtp import SMTPAlertBackend, SMTPAlertConfig


def _make_backend(**kwargs) -> SMTPAlertBackend:
    cfg = SMTPAlertConfig(
        host="smtp.example.com",
        port=587,
        from_addr="alerts@example.com",
        to_addrs=["ops@example.com"],
        **kwargs,
    )
    return SMTPAlertBackend(cfg)


def _make_event(**kwargs) -> AlertEvent:
    defaults = dict(
        pipeline_id="pipe-1",
        status="critical",
        message="Pipeline is stale",
        details={"last_run": "2024-01-01T00:00:00Z"},
    )
    defaults.update(kwargs)
    return AlertEvent(**defaults)


class TestSMTPAlertBackendSubject:
    def test_subject_contains_pipeline_id(self):
        backend = _make_backend()
        event = _make_event(pipeline_id="my-pipe")
        assert "my-pipe" in backend._build_subject(event)

    def test_subject_contains_status_upper(self):
        backend = _make_backend()
        event = _make_event(status="warning")
        assert "WARNING" in backend._build_subject(event)


class TestSMTPAlertBackendBody:
    def test_body_contains_pipeline_id(self):
        backend = _make_backend()
        event = _make_event(pipeline_id="etl-daily")
        assert "etl-daily" in backend._build_body(event)

    def test_body_contains_message(self):
        backend = _make_backend()
        event = _make_event(message="Row count too low")
        assert "Row count too low" in backend._build_body(event)

    def test_body_contains_details_keys(self):
        backend = _make_backend()
        event = _make_event(details={"rows": 0})
        body = backend._build_body(event)
        assert "rows" in body

    def test_body_no_details_section_when_empty(self):
        backend = _make_backend()
        event = _make_event(details={})
        assert "Details" not in backend._build_body(event)


class TestSMTPAlertBackendSend:
    def test_raises_when_to_addrs_empty(self):
        cfg = SMTPAlertConfig(to_addrs=[])
        backend = SMTPAlertBackend(cfg)
        with pytest.raises(ValueError, match="to_addrs"):
            backend.send(_make_event())

    def test_sendmail_called_with_correct_addresses(self):
        backend = _make_backend(use_tls=False)
        event = _make_event()
        mock_conn = MagicMock()
        mock_smtp = MagicMock(return_value=mock_conn)
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("pipewatch.backends.smtp.smtplib.SMTP", mock_smtp):
            backend.send(event)

        mock_conn.sendmail.assert_called_once()
        from_arg, to_arg, _ = mock_conn.sendmail.call_args[0]
        assert from_arg == "alerts@example.com"
        assert "ops@example.com" in to_arg

    def test_starttls_called_when_use_tls_true(self):
        backend = _make_backend(use_tls=True)
        mock_conn = MagicMock()
        mock_smtp = MagicMock(return_value=mock_conn)
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("pipewatch.backends.smtp.smtplib.SMTP", mock_smtp):
            backend.send(_make_event())

        mock_conn.starttls.assert_called_once()

    def test_login_called_when_credentials_provided(self):
        backend = _make_backend(username="user", password="secret", use_tls=False)
        mock_conn = MagicMock()
        mock_smtp = MagicMock(return_value=mock_conn)
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("pipewatch.backends.smtp.smtplib.SMTP", mock_smtp):
            backend.send(_make_event())

        mock_conn.login.assert_called_once_with("user", "secret")

    def test_login_not_called_when_no_credentials(self):
        backend = _make_backend(username="", password="", use_tls=False)
        mock_conn = MagicMock()
        mock_smtp = MagicMock(return_value=mock_conn)
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("pipewatch.backends.smtp.smtplib.SMTP", mock_smtp):
            backend.send(_make_event())

        mock_conn.login.assert_not_called()
