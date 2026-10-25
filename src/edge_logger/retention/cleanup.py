"""Retention predicates; actual deletion is performed only after audit persistence."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ..models import ReportPeriodState


def eligible_for_deletion(
    state: ReportPeriodState, email_sent_at: datetime | None, now: datetime, retention_days: int
) -> bool:
    if (
        state not in {ReportPeriodState.EMAIL_SENT, ReportPeriodState.RETENTION}
        or email_sent_at is None
    ):
        return False
    return now.astimezone(UTC) >= email_sent_at.astimezone(UTC) + timedelta(days=retention_days)
