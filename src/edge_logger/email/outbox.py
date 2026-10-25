"""Persistent SMTP outbox that never automatically resends an ambiguous send."""

from __future__ import annotations

import asyncio
import json
import os
import smtplib
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..models import EmailState
from ..storage.audit_db import AuditDatabase
from ..storage.raw_archive import archive_checksum
from .sender import (
    AttachmentTooLargeError,
    DeliveryNotStartedError,
    EmailConfiguration,
    send_report,
    send_test_message,
)


@dataclass(frozen=True, slots=True)
class EmailAttemptResult:
    report_id: str
    state: EmailState
    detail: str | None


class EmailOutboxProcessor:
    def __init__(self, configuration: dict[str, Any], database: AuditDatabase) -> None:
        self.configuration = configuration
        self.database = database

    @property
    def enabled(self) -> bool:
        return bool(self.configuration.get("enabled", False))

    def queue(self, period_id: str) -> None:
        if not self.enabled:
            return
        recipients = self.configuration.get("recipients", [])
        if not isinstance(recipients, list) or not all(
            isinstance(item, str) and item for item in recipients
        ):
            raise ValueError(
                "email.recipients must contain at least one recipient when email is enabled"
            )
        self.database.queue_email(period_id, json.dumps(recipients))

    def process_pending(self) -> tuple[EmailAttemptResult, ...]:
        if not self.enabled:
            return ()
        return tuple(
            self._send(str(row["report_period_id"])) for row in self.database.pending_email_outbox()
        )

    async def process_pending_async(self) -> tuple[EmailAttemptResult, ...]:
        """Keep SQLite on its owner thread and SMTP off the acquisition loop."""
        if not self.enabled:
            return ()
        results = []
        for row in self.database.pending_email_outbox():
            period_id = str(row["report_period_id"])
            period = dict(self.database.period(period_id))
            configuration = self._smtp_configuration()
            outbox = self.database.begin_email_attempt(period_id)
            configuration = self._recipient_configuration(
                configuration, str(outbox["recipients_json"])
            )
            export = self.database.application_state(f"process_export:{period_id}")
            delivery = asyncio.create_task(
                asyncio.to_thread(
                    self._deliver, period, configuration, str(outbox["message_id"]), export
                )
            )
            stopping = False
            try:
                result = await asyncio.shield(delivery)
            except asyncio.CancelledError:
                # Finish auditing an in-flight SMTP attempt during orderly shutdown.
                result = await delivery
                stopping = True
            self.database.finish_email_attempt(period_id, result.state, result.detail)
            results.append(result)
            if stopping:
                return tuple(results)
        return tuple(results)

    def retry(self, report_id: str) -> EmailAttemptResult:
        period = self.database.report_period(report_id)
        return self._send(str(period["id"]), allow_ambiguous=True)

    def send_test(self) -> str:
        if not self.enabled:
            raise RuntimeError("Email is disabled")
        return send_test_message(self._smtp_configuration())

    def _send(self, period_id: str, allow_ambiguous: bool = False) -> EmailAttemptResult:
        if not self.enabled:
            raise RuntimeError("Email is disabled")
        period = dict(self.database.period(period_id))
        configuration = self._smtp_configuration()
        outbox = self.database.begin_email_attempt(period_id, allow_ambiguous)
        configuration = self._recipient_configuration(configuration, str(outbox["recipients_json"]))
        export = self.database.application_state(f"process_export:{period_id}")
        result = self._deliver(period, configuration, str(outbox["message_id"]), export)
        self.database.finish_email_attempt(period_id, result.state, result.detail)
        return result

    @staticmethod
    def _recipient_configuration(
        configuration: EmailConfiguration, recipients_json: str
    ) -> EmailConfiguration:
        from dataclasses import replace

        return replace(configuration, recipients=tuple(json.loads(recipients_json)))

    @staticmethod
    def _deliver(
        period: dict[str, Any],
        configuration: EmailConfiguration,
        message_id_expected: str,
        export: str | None,
    ) -> EmailAttemptResult:
        report_id = str(period["report_id"])
        try:
            path = Path(str(period["workbook_path"]))
            if archive_checksum(path) != period["workbook_sha256"]:
                raise ValueError("Workbook checksum mismatch; delivery refused")
            records = None
            if export is not None:
                metadata = json.loads(export)
                records = Path(metadata["path"])
                if archive_checksum(records) != metadata["sha256"]:
                    raise ValueError("Process export checksum mismatch; delivery refused")
            message_id = send_report(
                configuration, report_id, path, "sealed report period", records
            )
        except (AttachmentTooLargeError, ValueError, smtplib.SMTPAuthenticationError) as error:
            return EmailAttemptResult(report_id, EmailState.FAILED_PERMANENT, str(error))
        except smtplib.SMTPRecipientsRefused as error:
            retryable = bool(error.recipients) and all(
                400 <= code < 500 for code, _ in error.recipients.values()
            )
            return EmailAttemptResult(
                report_id,
                EmailState.FAILED_RETRYABLE if retryable else EmailState.FAILED_PERMANENT,
                str(error),
            )
        except (
            DeliveryNotStartedError,
            ConnectionRefusedError,
            socket.gaierror,
            smtplib.SMTPConnectError,
        ) as error:
            return EmailAttemptResult(report_id, EmailState.FAILED_RETRYABLE, str(error))
        except (OSError, smtplib.SMTPException) as error:
            return EmailAttemptResult(report_id, EmailState.AMBIGUOUS, str(error))
        if message_id != message_id_expected:
            return EmailAttemptResult(
                report_id, EmailState.AMBIGUOUS, "SMTP Message-ID did not reconcile"
            )
        return EmailAttemptResult(report_id, EmailState.SENT, "SMTP accepted message")

    def _smtp_configuration(self) -> EmailConfiguration:
        password_name = self.configuration.get("password_env")
        password = os.environ.get(str(password_name)) if password_name else None
        if self.configuration.get("username") and not password:
            raise ValueError("SMTP password environment variable is missing")
        required = ("host", "sender")
        missing = [key for key in required if not self.configuration.get(key)]
        if missing:
            raise ValueError(f"Email configuration missing: {', '.join(missing)}")
        recipients = self.configuration.get("recipients", [])
        return EmailConfiguration(
            host=str(self.configuration["host"]),
            port=int(self.configuration.get("port", 587)),
            sender=str(self.configuration["sender"]),
            recipients=tuple(str(item) for item in recipients),
            username=str(self.configuration["username"])
            if self.configuration.get("username")
            else None,
            password=password,
            tls_mode=str(self.configuration.get("tls_mode", "starttls")),
            timeout_seconds=int(self.configuration.get("timeout_seconds", 30)),
            max_attachment_bytes=int(
                self.configuration.get("max_attachment_bytes", 18 * 1024 * 1024)
            ),
            max_message_bytes=int(self.configuration.get("max_message_bytes", 25 * 1024 * 1024)),
        )
