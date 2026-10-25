"""Synthetic fixtures only; no operational datasets are required."""

import math
from datetime import UTC, datetime, timedelta

from edge_logger.analysis.pipeline import analyse_records
from edge_logger.models import ProcessRecord, QualityStatus, SignalValue


def _record(
    cycle: str | None,
    value: object | None = 0,
    quality: QualityStatus = QualityStatus.GOOD,
    timestamp: datetime | None = None,
    received: datetime | None = None,
) -> ProcessRecord:
    source = timestamp if timestamp is not None else datetime(2026, 8, 20, 12, tzinfo=UTC)
    received_at = received if received is not None else source + timedelta(seconds=1)
    return ProcessRecord(
        "DEMO-01",
        source,
        received_at,
        cycle,
        f"fixture:{cycle}",
        (SignalValue("signal", "Signal", value, None, None, quality, source),),
    )


def _analysis(records: list[ProcessRecord]):
    return analyse_records(lambda: iter(records), {}, "Europe/London", 5, {})


def test_constant_classification_handles_zero_change_null_and_bad_quality() -> None:
    assert (
        _analysis([_record("1"), _record("2"), _record("3"), _record("4")])
        .profiles["signal"]
        .is_clean_constant
    )
    assert (
        not _analysis([_record("1"), _record("2"), _record("3", 1), _record("4")])
        .profiles["signal"]
        .is_clean_constant
    )
    null_profile = _analysis([_record("1"), _record("2", None), _record("3")]).profiles["signal"]
    assert not null_profile.is_clean_constant and null_profile.missing_readings == 1
    bad_profile = _analysis(
        [_record("1", 5), _record("2", 5, QualityStatus.BAD), _record("3", 5)]
    ).profiles["signal"]
    assert not bad_profile.is_clean_constant and bad_profile.bad_quality_readings == 1


def test_constant_classification_is_period_local() -> None:
    assert _analysis([_record("1", 5), _record("2", 5)]).profiles["signal"].is_clean_constant
    assert not _analysis([_record("3", 5), _record("4", 6)]).profiles["signal"].is_clean_constant


def test_strings_booleans_and_floats_use_exact_equality() -> None:
    assert (
        _analysis([_record("1", "ready"), _record("2", "ready")])
        .profiles["signal"]
        .is_clean_constant
    )
    assert _analysis([_record("1", True), _record("2", True)]).profiles["signal"].is_clean_constant
    assert _analysis([_record("1", 0.1), _record("2", 0.1)]).profiles["signal"].is_clean_constant
    changed = math.nextafter(0.1, 1.0)
    assert (
        not _analysis([_record("1", 0.1), _record("2", changed)])
        .profiles["signal"]
        .is_clean_constant
    )


def test_cycle_integrity_detects_duplicate_gap_and_reset() -> None:
    analysis = _analysis([_record("10"), _record("10"), _record("12"), _record("3")])
    assert analysis.duplicate_cycles == 1
    assert analysis.missing_cycles == 1
    assert analysis.event_counts["Data Quality:Cycle reset"] == 1


def test_cycle_integrity_accepts_sequential_and_tolerates_missing_or_non_numeric_ids() -> None:
    sequential = _analysis([_record("1"), _record("2"), _record("3")])
    assert sequential.missing_cycles == 0 and sequential.duplicate_cycles == 0
    mixed = _analysis([_record(None), _record("cycle-a"), _record("cycle-b")])
    assert mixed.missing_cycles == 0 and mixed.duplicate_cycles == 0


def test_duplicate_backward_and_missing_timestamps_are_events() -> None:
    first = datetime(2026, 8, 20, 12, tzinfo=UTC)
    records = [
        _record("1", timestamp=first),
        _record("2", timestamp=first),
        _record("3", timestamp=first - timedelta(seconds=1)),
        ProcessRecord("DEMO-01", None, first, "4", "fixture:4", ()),
    ]
    events = _analysis(records).event_counts
    assert events["Data Quality:Duplicate timestamp"] == 1
    assert events["Data Quality:Backwards timestamp"] == 1
    assert events["Data Quality:Missing timestamp"] == 1


def test_dst_transition_and_receive_timestamp_are_preserved() -> None:
    before = datetime(2026, 10, 25, 0, 55, tzinfo=UTC)
    after = datetime(2026, 10, 25, 1, 5, tzinfo=UTC)
    received = after + timedelta(seconds=2)
    analysis = _analysis(
        [_record("1", timestamp=before), _record("2", timestamp=after, received=received)]
    )
    assert analysis.record_count == 2
    assert analysis.first_timestamp == before and analysis.last_timestamp == after
    assert _record("2", timestamp=after, received=received).received_timestamp == received


def test_consistently_descending_export_is_reported_in_chronological_order() -> None:
    latest = datetime(2026, 8, 21, 12, tzinfo=UTC)
    records = [
        _record(str(100 - index), timestamp=latest - timedelta(seconds=index))
        for index in range(12)
    ]
    analysis = _analysis(records)
    assert analysis.first_timestamp == latest - timedelta(seconds=11)
    assert analysis.last_timestamp == latest
    assert analysis.first_cycle == "89" and analysis.last_cycle == "100"
    assert analysis.missing_cycles == 0
    assert analysis.event_counts["Data Quality:Backwards timestamp"] == 0
    assert analysis.event_counts["Data Quality:Cycle reset"] == 0


def test_cycle_trends_use_configured_intervals_in_chronological_order() -> None:
    latest = datetime(2026, 8, 21, 12, tzinfo=UTC)
    records = [
        _record(str(100 - index), timestamp=latest - timedelta(seconds=index))
        for index in range(12)
    ]
    analysis = analyse_records(
        lambda: iter(records),
        {"signal": {"category": "temperature", "trend": True}},
        "Europe/London",
        5,
        {},
        trend_bucket_count=3,
    )
    trends = analysis.trends["temperature"]
    assert [(row.interval, row.cycle_start, row.cycle_end) for row in trends] == [
        (1, 89, 99),
        (2, 100, 100),
    ]
    assert [row.start_timestamp for row in trends] == [
        latest - timedelta(seconds=11),
        latest,
    ]
