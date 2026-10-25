"""Synthetic fixtures only; no operational datasets are required."""

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path

from edge_logger.config import AppConfig, ReportingConfig
from edge_logger.models import ProcessRecord, ReportPeriodState
from edge_logger.runtime import LoggerRuntime
from edge_logger.storage.audit_db import AuditDatabase


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        machine_name="DEMO-01",
        timezone="Europe/London",
        config_version=1,
        reporting=ReportingConfig("acquisition_hours", 120, None, 5, True, 1),
        raw_path=tmp_path / "raw",
        reports_path=tmp_path / "reports",
        state_path=tmp_path / "state",
        rules={},
        signal_registry={},
        raw={"acquisition": {"provider": "none"}, "email": {"enabled": False}},
    )


def _record(cycle: str) -> ProcessRecord:
    now = datetime.now(UTC)
    return ProcessRecord("DEMO-01", now, now, cycle, f"fixture:{cycle}")


def _sealed(database: AuditDatabase, runtime: LoggerRuntime) -> object:
    asyncio.run(runtime.ingest_record(_record(str(time.monotonic())), 0))
    asyncio.run(runtime._close_active_segment())
    period = database.force_seal_open_period()
    assert period is not None
    return period


def test_successor_acquisition_continues_while_workbook_build_runs_in_thread(
    tmp_path: Path, monkeypatch
) -> None:
    database = AuditDatabase(tmp_path / "state.sqlite3")
    try:
        runtime = LoggerRuntime(_config(tmp_path), database)
        sealed = _sealed(database, runtime)

        def slow_report(*args: object) -> tuple[Path, str]:
            time.sleep(0.2)
            return tmp_path / "reports" / "report.xlsx", "checksum"

        monkeypatch.setattr("edge_logger.runtime.generate_report_from_archives", slow_report)

        async def exercise() -> None:
            task = asyncio.create_task(runtime.process_pending_reports())
            await asyncio.sleep(0.03)
            started = time.perf_counter()
            await runtime.ingest_record(_record("next"), 1)
            assert time.perf_counter() - started < 0.1
            assert database.open_period()["record_count"] == 1
            await task

        asyncio.run(exercise())
        assert database.period(sealed["id"])["state"] == ReportPeriodState.REPORT_VALIDATED
    finally:
        database.close()


def test_two_sealed_periods_are_serially_queued_without_dropping_open_records(
    tmp_path: Path, monkeypatch
) -> None:
    database = AuditDatabase(tmp_path / "state.sqlite3")
    try:
        runtime = LoggerRuntime(_config(tmp_path), database)
        first = _sealed(database, runtime)
        second = _sealed(database, runtime)
        calls: list[str] = []

        def report(*args: object) -> tuple[Path, str]:
            calls.append(str(args[2]))
            time.sleep(0.03)
            return tmp_path / "reports" / f"{len(calls)}.xlsx", "checksum"

        monkeypatch.setattr("edge_logger.runtime.generate_report_from_archives", report)
        asyncio.run(runtime.process_pending_reports())
        assert len(calls) == 2
        assert database.period(first["id"])["state"] == ReportPeriodState.REPORT_VALIDATED
        assert database.period(second["id"])["state"] == ReportPeriodState.REPORT_VALIDATED
        assert database.open_period()["state"] == ReportPeriodState.OPEN
    finally:
        database.close()
