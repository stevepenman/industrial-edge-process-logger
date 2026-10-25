"""Synthetic fixtures only; no operational datasets are required."""

from datetime import UTC, datetime

from edge_logger.analysis.constants import classify_signals
from edge_logger.models import ProcessRecord, QualityStatus, SignalValue


def _record(value: object | None, quality: QualityStatus = QualityStatus.GOOD) -> ProcessRecord:
    timestamp = datetime.now(UTC)
    return ProcessRecord(
        machine_name="DEMO-01",
        source_timestamp=timestamp,
        received_timestamp=timestamp,
        cycle_id="1",
        source_identifier="test",
        signals=(SignalValue("tcd05", "TCD05", value, "C", "float", quality, timestamp),),
    )


def test_zero_values_are_clean_constant() -> None:
    result = classify_signals([_record(0), _record(0), _record(0), _record(0)])
    assert result["tcd05"].is_clean_constant


def test_null_value_prevents_clean_constant() -> None:
    result = classify_signals([_record(0), _record(0), _record(None), _record(0)])
    assert not result["tcd05"].is_clean_constant
