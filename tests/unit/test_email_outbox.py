"""Synthetic fixtures only; no operational datasets are required."""

import smtplib
from pathlib import Path

import pytest

from edge_logger.email.outbox import EmailOutboxProcessor
from edge_logger.email.sender import AttachmentTooLargeError, EmailConfiguration, send_report
from edge_logger.models import EmailState, ReportPeriodState
from edge_logger.storage.audit_db import AuditDatabase
from edge_logger.storage.raw_archive import archive_checksum


def _period(database: AuditDatabase, workbook: Path) -> object:
    database.increment_record_count()
    sealed = database.force_seal_open_period()
    assert sealed is not None
    database.transition_period(sealed["id"], ReportPeriodState.REPORT_BUILDING)
    database.transition_period(sealed["id"], ReportPeriodState.REPORT_VALIDATED)
    workbook.write_bytes(b"report")
    database.set_workbook(sealed["id"], workbook, archive_checksum(workbook))
    return sealed


def _processor(database: AuditDatabase) -> EmailOutboxProcessor:
    return EmailOutboxProcessor(
        {
            "enabled": True,
            "host": "smtp.example.test",
            "port": 587,
            "sender": "logger@example.test",
            "recipients": ["operator@example.test"],
        },
        database,
    )


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (ConnectionRefusedError("refused"), EmailState.FAILED_RETRYABLE),
        (TimeoutError("timeout after submission may have started"), EmailState.AMBIGUOUS),
        (smtplib.SMTPRecipientsRefused({}), EmailState.FAILED_PERMANENT),
        (AttachmentTooLargeError("too large"), EmailState.FAILED_PERMANENT),
        (smtplib.SMTPServerDisconnected("lost after DATA"), EmailState.AMBIGUOUS),
    ],
)
def test_email_failure_states_are_conservative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: Exception, expected: EmailState
) -> None:
    database = AuditDatabase(tmp_path / "state.sqlite3")
    try:
        period = _period(database, tmp_path / "report.xlsx")
        processor = _processor(database)
        processor.queue(period["id"])
        monkeypatch.setattr(
            "edge_logger.email.outbox.send_report", lambda *_: (_ for _ in ()).throw(failure)
        )
        result = processor.process_pending()[0]
        assert result.state == expected
        if expected is EmailState.AMBIGUOUS:
            assert database.pending_email_outbox() == []
            with pytest.raises(ValueError):
                database.begin_email_attempt(period["id"])
    finally:
        database.close()


def test_stable_message_id_and_operator_retry_are_explicit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = AuditDatabase(tmp_path / "state.sqlite3")
    try:
        period = _period(database, tmp_path / "report.xlsx")
        processor = _processor(database)
        processor.queue(period["id"])
        initial = database.email_outbox(period["id"])["message_id"]
        monkeypatch.setattr(
            "edge_logger.email.outbox.send_report",
            lambda *_: (_ for _ in ()).throw(smtplib.SMTPServerDisconnected("lost")),
        )
        assert processor.process_pending()[0].state == EmailState.AMBIGUOUS
        monkeypatch.setattr("edge_logger.email.outbox.send_report", lambda *_: initial)
        result = processor.retry(str(period["report_id"]))
        assert result.state == EmailState.SENT
        assert database.email_outbox(period["id"])["message_id"] == initial
    finally:
        database.close()


def test_email_test_sends_no_report_attachment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = AuditDatabase(tmp_path / "state.sqlite3")
    try:
        processor = _processor(database)
        monkeypatch.setattr(
            "edge_logger.email.outbox.send_test_message", lambda *_: "<smtp-test@edge-logger.local>"
        )
        assert processor.send_test() == "<smtp-test@edge-logger.local>"
        assert database.periods() == []
    finally:
        database.close()


def test_attachment_limit_is_checked_before_any_smtp_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workbook = tmp_path / "report.xlsx"
    workbook.write_bytes(b"too large")
    attempted = False

    def smtp(*_: object, **__: object) -> object:
        nonlocal attempted
        attempted = True
        raise AssertionError("SMTP must not be connected for an oversized attachment")

    monkeypatch.setattr("edge_logger.email.sender.smtplib.SMTP", smtp)
    with pytest.raises(AttachmentTooLargeError, match="max_attachment_bytes"):
        send_report(
            EmailConfiguration(
                "smtp.example.test",
                587,
                "logger@example.test",
                ("operator@example.test",),
                max_attachment_bytes=1,
            ),
            "report-id",
            workbook,
            "test",
        )
    assert not attempted
