"""Synthetic fixtures only; no operational datasets are required."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from edge_logger.models import ProcessRecord, QualityStatus, SignalValue
from edge_logger.reporting.generator import ReportGenerationError, generate_report
from edge_logger.reporting.validator import WorkbookValidation


def _records() -> list[ProcessRecord]:
    timestamp = datetime(2026, 8, 20, 12, tzinfo=UTC)
    return [
        ProcessRecord(
            "DEMO-01",
            timestamp,
            timestamp,
            "1",
            "fixture:1",
            (
                SignalValue(
                    "temperature", "Temperature", 50.0, "C", "float", QualityStatus.GOOD, timestamp
                ),
            ),
        )
    ]


@pytest.mark.parametrize("boundary", ["workbook_close", "validation", "checksum", "rename"])
def test_report_is_not_promoted_when_a_transaction_boundary_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    report_id = "f5e93f44-15e0-4b33-8ce3-b74a0edbead0"
    if boundary == "workbook_close":
        original_close = __import__("xlsxwriter").Workbook.close

        def close_then_fail(workbook: object) -> None:
            original_close(workbook)
            raise OSError("workbook close fault")

        monkeypatch.setattr(
            "edge_logger.reporting.generator.xlsxwriter.Workbook.close", close_then_fail
        )
        expected: type[Exception] = OSError
    elif boundary == "validation":
        monkeypatch.setattr(
            "edge_logger.reporting.generator.validate_workbook",
            lambda *_: WorkbookValidation(False, ("validation fault",)),
        )
        expected = ReportGenerationError
    elif boundary == "checksum":
        monkeypatch.setattr(
            "edge_logger.reporting.generator._sha256",
            lambda *_: (_ for _ in ()).throw(OSError("checksum fault")),
        )
        expected = OSError
    else:
        monkeypatch.setattr(
            "edge_logger.reporting.generator.os.replace",
            lambda *_: (_ for _ in ()).throw(OSError("rename fault")),
        )
        expected = OSError
    with pytest.raises(expected):
        generate_report(iter(_records()), tmp_path, report_id, "DEMO-01", "Europe/London", {})
    assert not list(tmp_path.glob("DEMO-01_process_report_*.xlsx"))


def test_iterator_backed_generation_uses_a_temporary_raw_archive_and_cleans_it(
    tmp_path: Path,
) -> None:
    report_id = "f5e93f44-15e0-4b33-8ce3-b74a0edbead0"
    report, _ = generate_report(
        iter(_records()), tmp_path, report_id, "DEMO-01", "Europe/London", {}
    )
    assert report.exists()
    assert not list(tmp_path.glob(".*.input.jsonl"))
