"""SMTP delivery that refuses automatic ambiguous-send retries."""

from __future__ import annotations

import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path


@dataclass(frozen=True, slots=True)
class EmailConfiguration:
    host: str
    port: int
    sender: str
    recipients: tuple[str, ...]
    username: str | None = None
    password: str | None = None
    tls_mode: str = "starttls"
    timeout_seconds: int = 30
    max_attachment_bytes: int = 18 * 1024 * 1024
    max_message_bytes: int = 25 * 1024 * 1024


class AttachmentTooLargeError(ValueError):
    """The configured SMTP limit makes delivery futile until an operator acts."""


class DeliveryNotStartedError(OSError):
    """A transport failure before message submission; safe to retry."""


class PartialDeliveryError(smtplib.SMTPException):
    """Some recipients accepted the message; resending could duplicate delivery."""


def send_report(
    configuration: EmailConfiguration,
    report_id: str,
    workbook: Path,
    period_description: str,
    records: Path | None = None,
) -> str:
    """Send once; callers must persist EMAIL_SENDING before invoking this function."""
    size = workbook.stat().st_size + (records.stat().st_size if records is not None else 0)
    if size > configuration.max_attachment_bytes:
        raise AttachmentTooLargeError(
            f"Workbook is {size} bytes; email.max_attachment_bytes is {configuration.max_attachment_bytes}"
        )
    message = EmailMessage()
    message["From"] = configuration.sender
    message["To"] = ", ".join(configuration.recipients)
    message["Subject"] = f"DEMO-01 Process Report - {report_id} - {period_description}"
    message["Message-ID"] = f"<{report_id}@edge-logger.local>"
    message.set_content("DEMO-01 process report attached.")
    message.add_attachment(
        workbook.read_bytes(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=workbook.name,
    )
    if records is not None:
        message.add_attachment(
            records.read_bytes(), maintype="application", subtype="gzip", filename=records.name
        )
    _send_message(configuration, message)
    return str(message["Message-ID"])


def send_test_message(configuration: EmailConfiguration) -> str:
    """Prove SMTP credentials without mutating any production report state."""
    message = EmailMessage()
    message["From"] = configuration.sender
    message["To"] = ", ".join(configuration.recipients)
    message["Subject"] = "DEMO-01 logger SMTP commissioning test"
    message["Message-ID"] = "<smtp-test@edge-logger.local>"
    message.set_content("DEMO-01 logger SMTP commissioning test. No production report is attached.")
    _send_message(configuration, message)
    return str(message["Message-ID"])


def _send_message(configuration: EmailConfiguration, message: EmailMessage) -> None:
    if len(message.as_bytes()) > configuration.max_message_bytes:
        raise AttachmentTooLargeError("Encoded email exceeds email.max_message_bytes")
    client_type: type[smtplib.SMTP | smtplib.SMTP_SSL]
    client_type = smtplib.SMTP_SSL if configuration.tls_mode == "ssl" else smtplib.SMTP
    client: smtplib.SMTP
    try:
        if configuration.tls_mode == "ssl":
            client = smtplib.SMTP_SSL(
                configuration.host,
                configuration.port,
                timeout=configuration.timeout_seconds,
                context=ssl.create_default_context(),
            )
        else:
            client = client_type(
                configuration.host, configuration.port, timeout=configuration.timeout_seconds
            )
    except (TimeoutError, OSError, smtplib.SMTPConnectError) as error:
        raise DeliveryNotStartedError(str(error)) from error
    with client:
        try:
            if configuration.tls_mode == "starttls":
                client.starttls(context=ssl.create_default_context())
            if configuration.username:
                if not configuration.password:
                    raise ValueError("SMTP password environment variable is missing")
                client.login(configuration.username, configuration.password)
        except (TimeoutError, ConnectionError) as error:
            raise DeliveryNotStartedError(str(error)) from error
        refused = client.send_message(message)
        if refused:
            raise PartialDeliveryError(
                f"Partial recipient refusal: {refused}; accepted recipients must not be automatically resent"
            )
