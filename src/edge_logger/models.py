"""Canonical, timestamp-preserving models used throughout the logger."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class QualityStatus(StrEnum):
    GOOD = "Good"
    UNCERTAIN = "Uncertain"
    BAD = "Bad"
    MISSING = "Missing"
    PARSE_ERROR = "ParseError"
    INVALID_NUMERIC = "InvalidNumeric"
    STALE = "Stale"


class ReportPeriodState(StrEnum):
    OPEN = "OPEN"
    DUE = "DUE"
    SEALED = "SEALED"
    REPORT_BUILDING = "REPORT_BUILDING"
    REPORT_VALIDATED = "REPORT_VALIDATED"
    EMAIL_PENDING = "EMAIL_PENDING"
    EMAIL_SENDING = "EMAIL_SENDING"
    EMAIL_SENT = "EMAIL_SENT"
    EMAIL_AMBIGUOUS = "EMAIL_AMBIGUOUS"
    RETENTION = "RETENTION"
    FILES_DELETED = "FILES_DELETED"
    ERROR = "ERROR"


class EmailState(StrEnum):
    PENDING = "PENDING"
    SENDING = "SENDING"
    SENT = "SENT"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_PERMANENT = "FAILED_PERMANENT"
    AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True, slots=True)
class SignalValue:
    signal_id: str
    display_name: str
    value: Any
    unit: str | None
    datatype: str | None
    quality: QualityStatus
    source_timestamp: datetime | None
    server_timestamp: datetime | None = None
    status_code: str | None = None
    source_name: str | None = None
    raw_value: str | None = None


@dataclass(frozen=True, slots=True)
class ProcessRecord:
    machine_name: str
    source_timestamp: datetime | None
    received_timestamp: datetime
    cycle_id: str | None
    source_identifier: str
    signals: tuple[SignalValue, ...] = field(default_factory=tuple)
    run_id: str | None = None
    production_context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Event:
    timestamp: datetime
    category: str
    event_type: str
    severity: str
    detail: str
    cycle_id: str | None = None
    signal_id: str | None = None
    value: Any | None = None
    threshold: Any | None = None
    source: str | None = None
