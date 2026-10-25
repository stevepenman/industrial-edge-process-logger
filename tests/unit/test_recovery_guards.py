"""Synthetic fixtures only; no operational datasets are required."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from edge_logger.models import EmailState, ProcessRecord, ReportPeriodState
from edge_logger.retention.executor import run_retention
from edge_logger.storage.audit_db import AuditDatabase
from edge_logger.storage.raw_archive import RawArchiveWriter, read_records
from edge_logger.storage.source_archive import archive_source_file


def _record() -> ProcessRecord:
    now = datetime.now(UTC)
    return ProcessRecord("DEMO-01", now, now, "1", "fixture:1")


def _validated_period(database: AuditDatabase) -> object:
    database.increment_record_count()
    sealed = database.force_seal_open_period()
    assert sealed is not None
    database.transition_period(sealed["id"], ReportPeriodState.REPORT_BUILDING)
    database.transition_period(sealed["id"], ReportPeriodState.REPORT_VALIDATED)
    return sealed


def test_raw_archive_promotes_only_after_fsync_and_close(tmp_path: Path) -> None:
    writer = RawArchiveWriter(tmp_path, "segment")
    writer.append(_record())
    assert not writer.path.exists()
    assert (tmp_path / ".segment.partial.jsonl").is_file()
    checksum = writer.close()
    assert writer.path.is_file() and checksum
    assert next(iter(read_records(writer.path))).cycle_id == "1"


def test_source_archive_failure_cleans_temporary_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.tsv"
    source.write_bytes(b"source")
    archive = tmp_path / "archive"
    monkeypatch.setattr(
        "edge_logger.storage.source_archive.os.replace",
        lambda *_: (_ for _ in ()).throw(OSError("fault")),
    )
    with pytest.raises(OSError, match="fault"):
        archive_source_file(source, archive)
    assert source.read_bytes() == b"source"
    assert not list(archive.glob("*.tmp"))


def test_interrupted_report_build_is_resealed_for_exactly_one_rebuild(tmp_path: Path) -> None:
    database = AuditDatabase(tmp_path / "state.sqlite3")
    try:
        sealed = _validated_period(database)
        database.transition_period(sealed["id"], ReportPeriodState.EMAIL_PENDING)
        # A report build left mid-flight is only possible before validation; model that state directly.
        database.connection.execute(
            "UPDATE report_periods SET state = ? WHERE id = ?",
            (ReportPeriodState.REPORT_BUILDING.value, sealed["id"]),
        )
        recovered = database.reset_interrupted_report_builds()
        assert [row["id"] for row in recovered] == [sealed["id"]]
        assert database.period(sealed["id"])["state"] == ReportPeriodState.SEALED
        assert database.reset_interrupted_report_builds() == []
    finally:
        database.close()


def test_interrupted_email_send_is_ambiguous_and_not_auto_pending(tmp_path: Path) -> None:
    database = AuditDatabase(tmp_path / "state.sqlite3")
    try:
        sealed = _validated_period(database)
        database.queue_email(sealed["id"], "[]")
        database.begin_email_attempt(sealed["id"])
        assert database.recover_interrupted_email_sends() == 1
        assert database.email_outbox(sealed["id"])["state"] == EmailState.AMBIGUOUS
        assert database.pending_email_outbox() == []
        assert database.recover_interrupted_email_sends() == 0
    finally:
        database.close()


def test_retention_missing_file_becomes_audited_error(tmp_path: Path) -> None:
    database = AuditDatabase(tmp_path / "state.sqlite3")
    try:
        sealed = _validated_period(database)
        report = tmp_path / "reports" / "missing.xlsx"
        database.set_workbook(sealed["id"], report, "checksum")
        database.queue_email(sealed["id"], "[]")
        database.begin_email_attempt(sealed["id"])
        database.finish_email_attempt(sealed["id"], EmailState.SENT)
        database.acknowledge_storage(sealed["report_id"], "checksum", "verified receiver")
        sent = datetime.fromisoformat(database.period(sealed["id"])["email_sent_at"])
        result = run_retention(
            database, tmp_path / "reports", tmp_path / "raw", 30, sent + timedelta(days=30), False
        )
        assert not result[0].eligible
        assert "missing" in result[0].reason
        events = list(database.connection.execute("SELECT detail FROM system_events"))
        assert events and "missing" in events[0]["detail"]
    finally:
        database.close()
