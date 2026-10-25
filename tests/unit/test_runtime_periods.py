"""Synthetic fixtures only; no operational datasets are required."""

from pathlib import Path

from edge_logger.models import ReportPeriodState
from edge_logger.storage.audit_db import AuditDatabase


def test_runtime_survives_restart_and_seals_only_once(tmp_path: Path) -> None:
    database_path = tmp_path / "state.sqlite3"
    database = AuditDatabase(database_path)
    database.add_acquisition_seconds(48 * 3600)
    assert database.seal_due_period(120 * 3600) is None
    database.close()

    database = AuditDatabase(database_path)
    database.add_acquisition_seconds(72 * 3600)
    sealed = database.seal_due_period(120 * 3600)
    assert sealed is not None
    assert sealed["state"] == ReportPeriodState.SEALED
    assert database.seal_due_period(120 * 3600) is None
    assert [period["state"] for period in database.periods()] == [
        ReportPeriodState.SEALED,
        ReportPeriodState.OPEN,
    ]
    database.close()
