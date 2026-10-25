"""Explicit report period lifecycle transitions."""

from __future__ import annotations

from .models import ReportPeriodState


class InvalidPeriodTransition(ValueError):
    """Raised when a report period would enter an unsafe state."""


_TRANSITIONS: dict[ReportPeriodState, set[ReportPeriodState]] = {
    ReportPeriodState.OPEN: {ReportPeriodState.DUE, ReportPeriodState.ERROR},
    ReportPeriodState.DUE: {ReportPeriodState.SEALED, ReportPeriodState.ERROR},
    ReportPeriodState.SEALED: {ReportPeriodState.REPORT_BUILDING, ReportPeriodState.ERROR},
    ReportPeriodState.REPORT_BUILDING: {
        ReportPeriodState.REPORT_VALIDATED,
        ReportPeriodState.ERROR,
    },
    ReportPeriodState.REPORT_VALIDATED: {ReportPeriodState.EMAIL_PENDING, ReportPeriodState.ERROR},
    ReportPeriodState.EMAIL_PENDING: {ReportPeriodState.EMAIL_SENDING, ReportPeriodState.ERROR},
    ReportPeriodState.EMAIL_SENDING: {
        ReportPeriodState.EMAIL_SENT,
        ReportPeriodState.EMAIL_PENDING,
        ReportPeriodState.EMAIL_AMBIGUOUS,
        ReportPeriodState.ERROR,
    },
    ReportPeriodState.EMAIL_SENT: {ReportPeriodState.RETENTION, ReportPeriodState.ERROR},
    ReportPeriodState.EMAIL_AMBIGUOUS: {ReportPeriodState.ERROR},
    ReportPeriodState.RETENTION: {ReportPeriodState.FILES_DELETED, ReportPeriodState.ERROR},
    ReportPeriodState.FILES_DELETED: set(),
    ReportPeriodState.ERROR: set(),
}


def validate_transition(current: ReportPeriodState, target: ReportPeriodState) -> None:
    if target not in _TRANSITIONS[current]:
        raise InvalidPeriodTransition(f"Cannot transition report period from {current} to {target}")
