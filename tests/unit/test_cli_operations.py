"""Synthetic fixtures only; no operational datasets are required."""

import json
from pathlib import Path

from edge_logger.cli import main
from edge_logger.storage.audit_db import AuditDatabase


def _config(path: Path) -> Path:
    config = path / "config.yaml"
    config.write_text(
        "\n".join(
            [
                "machine:",
                "  name: DEMO-01",
                "  timezone: Europe/London",
                "application:",
                "  config_version: 1",
                "reporting:",
                "  trigger_mode: acquisition_hours",
                "  acquisition_hours: 120",
                "  maximum_calendar_days: null",
                "  trend_bucket_minutes: 5",
                "  generate_if_due_on_startup: true",
                "paths:",
                f"  raw: {path / 'raw'}",
                f"  reports: {path / 'reports'}",
                f"  state: {path / 'state'}",
                "retention:",
                "  days_after_successful_email: 30",
                "ups:",
                "  shutdown_delay_seconds: 90",
                "email:",
                "  enabled: false",
                "opcua:",
                "  endpoint: null",
                "signals: {}",
            ]
        ),
        encoding="utf-8",
    )
    return config


def test_status_json_and_read_only_db_check(tmp_path: Path, capsys) -> None:
    config = _config(tmp_path)
    database = AuditDatabase(tmp_path / "state" / "state.sqlite3")
    database.close()
    assert main(["--config", str(config), "status", "--json"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["application_version"] == "0.1.0"
    assert status["current_report_period_id"] is None
    assert status["sqlite_durability"]["journal_mode"] == "wal"
    assert main(["--config", str(config), "db-check"]) == 0
    check = json.loads(capsys.readouterr().out)
    assert check["valid"] and check["integrity_check"] == ["ok"]
