"""Synthetic fixtures only; no operational datasets are required."""

from pathlib import Path

from edge_logger.storage.audit_db import AuditDatabase


def test_durability_settings_and_read_only_integrity_check(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    database = AuditDatabase(path)
    try:
        settings = database.durability_settings()
        assert settings["journal_mode"] == "wal"
        assert settings["synchronous"] == 2
        assert settings["busy_timeout_ms"] == 5_000
        assert settings["foreign_keys"] == 1
    finally:
        database.close()
    result = AuditDatabase.read_only_check(path)
    assert result["valid"]
    assert result["integrity_check"] == ("ok",)
    assert result["foreign_key_violations"] == ()


def test_status_query_does_not_create_open_period(tmp_path: Path) -> None:
    database = AuditDatabase(tmp_path / "state.sqlite3")
    try:
        assert database.open_period() is None
        assert database.periods() == []
    finally:
        database.close()
