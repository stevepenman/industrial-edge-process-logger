"""Synthetic fixtures only; no operational datasets are required."""

from pathlib import Path

import pytest

from edge_logger.models import ReportPeriodState
from edge_logger.storage.audit_db import AuditDatabase

HOUR = 3_600.0
THRESHOLD = 120 * HOUR


@pytest.fixture
def database(tmp_path: Path) -> AuditDatabase:
    database = AuditDatabase(tmp_path / "state.sqlite3")
    yield database
    database.close()


def test_119_point_9_hours_does_not_become_due(database: AuditDatabase) -> None:
    database.record_acquisition(1, 119.9 * HOUR)
    assert database.mark_due_period(THRESHOLD) is None
    assert database.open_period()["state"] == ReportPeriodState.OPEN


def test_exactly_120_hours_seals_once_and_opens_successor(database: AuditDatabase) -> None:
    database.record_acquisition(1, THRESHOLD)
    sealed = database.seal_due_period(THRESHOLD)
    assert sealed is not None
    assert sealed["state"] == ReportPeriodState.SEALED
    assert database.seal_due_period(THRESHOLD) is None
    periods = database.periods()
    assert [period["state"] for period in periods] == [
        ReportPeriodState.SEALED,
        ReportPeriodState.OPEN,
    ]


def test_48_hours_then_restart_then_72_hours_is_one_period(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    first = AuditDatabase(path)
    first.record_acquisition(3, 48 * HOUR)
    first.close()
    second = AuditDatabase(path)
    try:
        second.record_acquisition(4, 72 * HOUR)
        sealed = second.seal_due_period(THRESHOLD)
        assert sealed is not None
        assert sealed["record_count"] == 7
        assert len(second.periods()) == 2
    finally:
        second.close()


def test_119_point_9_hours_crash_restart_then_point_1_hours_seals_once(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    first = AuditDatabase(path)
    first.record_acquisition(1, 119.9 * HOUR)
    first.close()
    second = AuditDatabase(path)
    try:
        second.record_acquisition(1, 0.1 * HOUR)
        sealed = second.seal_due_period(THRESHOLD)
        assert sealed is not None
        assert second.seal_due_period(THRESHOLD) is None
    finally:
        second.close()


def test_due_state_survives_restart(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    first = AuditDatabase(path)
    first.record_acquisition(1, THRESHOLD)
    due = first.mark_due_period(THRESHOLD)
    assert due is not None and due["state"] == ReportPeriodState.DUE
    first.close()
    second = AuditDatabase(path)
    try:
        assert second.open_period() is None
        sealed = second.seal_due_period(THRESHOLD)
        assert sealed is not None
        assert second.open_period()["state"] == ReportPeriodState.OPEN
    finally:
        second.close()


def test_records_on_each_side_of_boundary_belong_to_one_period(database: AuditDatabase) -> None:
    database.record_acquisition(1, THRESHOLD - 1)
    database.record_acquisition(1, 1)
    sealed = database.seal_due_period(THRESHOLD)
    assert sealed is not None
    database.record_acquisition(1, 1)
    successor = database.open_period()
    assert sealed["record_count"] == 2
    assert successor["record_count"] == 1


def test_acquisition_downtime_does_not_increment_counter(database: AuditDatabase) -> None:
    period = database.get_or_create_open_period()
    assert period["accumulated_seconds"] == 0
    # No record means no call to record_acquisition, regardless of wall-clock passage.
    assert database.open_period()["accumulated_seconds"] == 0
