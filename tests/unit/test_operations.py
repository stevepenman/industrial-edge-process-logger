"""Synthetic fixtures only; no operational datasets are required."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from edge_logger.config import AppConfig, ReportingConfig
from edge_logger.models import EmailState, ProcessRecord, ReportPeriodState
from edge_logger.retention.executor import run_retention
from edge_logger.runtime import LoggerRuntime
from edge_logger.storage.audit_db import AuditDatabase
from edge_logger.storage.raw_archive import archive_checksum
from edge_logger.ups.base import UpsStatus
from edge_logger.ups.monitor import UpsShutdownController


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        machine_name="DEMO-01",
        timezone="Europe/London",
        config_version=1,
        reporting=ReportingConfig("acquisition_hours", 120, None, 5, True, 1),
        raw_path=tmp_path / "raw",
        reports_path=tmp_path / "reports",
        state_path=tmp_path / "state",
        rules={"long_cycle_seconds": 12},
        signal_registry={},
        raw={
            "retention": {"days_after_successful_email": 30},
            "ups": {"shutdown_delay_seconds": 90},
            "email": {"enabled": False},
        },
    )


def _record(cycle: str) -> ProcessRecord:
    timestamp = datetime(2026, 8, 20, 12, tzinfo=UTC)
    return ProcessRecord("DEMO-01", timestamp, timestamp, cycle, f"test:{cycle}")


def test_runtime_seals_boundary_and_opens_successor(tmp_path: Path) -> None:
    config = _config(tmp_path)
    database = AuditDatabase(config.state_path / "state.sqlite3")
    try:
        runtime = LoggerRuntime(config, database)
        assert asyncio.run(runtime.ingest_record(_record("1"), 48 * 3600)) is None
        sealed = asyncio.run(runtime.ingest_record(_record("2"), 72 * 3600))
        assert sealed is not None
        assert sealed["state"] == ReportPeriodState.SEALED
        assert len(database.raw_segments_for_period(sealed["id"])) == 1
        assert [period["state"] for period in database.periods()] == [
            ReportPeriodState.SEALED,
            ReportPeriodState.OPEN,
        ]
    finally:
        database.close()


def test_ups_controller_waits_and_cancels() -> None:
    controller = UpsShutdownController(90)
    battery = UpsStatus(external_power_present=False, battery_critical=False)
    assert not controller.observe(battery, 0)
    assert not controller.observe(battery, 89)
    assert not controller.observe(UpsStatus(True, False), 89.5)
    assert not controller.observe(battery, 90)
    assert controller.observe(battery, 181)
    assert UpsShutdownController(90).observe(UpsStatus(False, True), 0)


def test_retention_only_deletes_confirmed_email_files(tmp_path: Path) -> None:
    config = _config(tmp_path)
    database = AuditDatabase(config.state_path / "state.sqlite3")
    try:
        database.get_or_create_open_period()
        database.increment_record_count()
        sealed = database.force_seal_open_period()
        assert sealed is not None
        database.transition_period(sealed["id"], ReportPeriodState.REPORT_BUILDING)
        database.transition_period(sealed["id"], ReportPeriodState.REPORT_VALIDATED)
        report = config.reports_path / "report.xlsx"
        report.parent.mkdir(parents=True)
        report.write_bytes(b"report")
        raw = config.raw_path / "records" / "segment.jsonl"
        raw.parent.mkdir(parents=True)
        raw.write_bytes(b"raw")
        database.set_workbook(sealed["id"], report, archive_checksum(report))
        database.register_raw_segment(sealed["id"], raw, archive_checksum(raw), 1)
        database.queue_email(sealed["id"], "[]")
        database.begin_email_attempt(sealed["id"])
        database.finish_email_attempt(sealed["id"], EmailState.SENT)
        sent_at = datetime.fromisoformat(database.period(sealed["id"])["email_sent_at"])
        results = run_retention(
            database,
            config.reports_path,
            config.raw_path,
            30,
            sent_at + timedelta(days=29),
            dry_run=False,
        )
        assert not results[0].eligible
        assert report.exists() and raw.exists()
        without_receipt = run_retention(
            database,
            config.reports_path,
            config.raw_path,
            30,
            sent_at + timedelta(days=31),
            dry_run=False,
        )
        assert not without_receipt[0].eligible
        database.acknowledge_storage(
            sealed["report_id"], archive_checksum(report), "receiver-verified"
        )
        results = run_retention(
            database,
            config.reports_path,
            config.raw_path,
            30,
            sent_at + timedelta(days=31),
            dry_run=False,
        )
        assert results[0].eligible
        assert not report.exists() and not raw.exists()
    finally:
        database.close()


def test_ambiguous_email_requires_deliberate_retry(tmp_path: Path) -> None:
    database = AuditDatabase(tmp_path / "state.sqlite3")
    try:
        database.get_or_create_open_period()
        database.increment_record_count()
        sealed = database.force_seal_open_period()
        assert sealed is not None
        database.transition_period(sealed["id"], ReportPeriodState.REPORT_BUILDING)
        database.transition_period(sealed["id"], ReportPeriodState.REPORT_VALIDATED)
        database.queue_email(sealed["id"], "[]")
        database.begin_email_attempt(sealed["id"])
        database.finish_email_attempt(sealed["id"], EmailState.AMBIGUOUS, "connection dropped")
        with pytest.raises(ValueError):
            database.begin_email_attempt(sealed["id"])
        assert (
            database.begin_email_attempt(sealed["id"], allow_ambiguous=True)["state"]
            == EmailState.SENDING
        )
    finally:
        database.close()
