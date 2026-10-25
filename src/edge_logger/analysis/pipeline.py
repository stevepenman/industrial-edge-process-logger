"""Streaming report-period analysis and deterministic event generation.

Severity is intentionally conservative for the initial commissioning phase:
process and data-quality findings are ``Warning``; a required configured signal
missing from a complete report is ``Error``. Site/OEM policy can later refine
the mapping through the signal registry without changing the raw data path.
"""

from __future__ import annotations

import math
import time
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ..metrics import PipelineMetrics
from ..models import Event, ProcessRecord, QualityStatus, SignalValue
from .parts import PartTotals

RecordFactory = Callable[[], Iterator[ProcessRecord]]
_MAX_TRACKED_UNIQUE_VALUES = 4_096


@dataclass(frozen=True, slots=True)
class SignalProfile:
    signal_id: str
    display_name: str
    unit: str | None
    category: str
    metadata: dict[str, object]
    rows: int
    valid_readings: int
    missing_readings: int
    bad_quality_readings: int
    uncertain_quality_readings: int
    parse_failures: int
    unique_values: int
    minimum: float | None
    average: float | None
    standard_deviation: float | None
    maximum: float | None
    zero_count: int
    low_limit_violations: int
    high_limit_violations: int
    constant_value: object | None
    is_clean_constant: bool
    quality_summary: str

    @property
    def is_numeric(self) -> bool:
        return self.minimum is not None

    @property
    def status(self) -> str:
        if (
            self.missing_readings
            or self.bad_quality_readings
            or self.uncertain_quality_readings
            or self.parse_failures
        ):
            return "Data quality review"
        return "Constant" if self.is_clean_constant else "Active"


@dataclass(frozen=True, slots=True)
class TrendRow:
    interval: int
    cycle_start: int | None
    cycle_end: int | None
    start_timestamp: datetime | None
    end_timestamp: datetime | None
    values: dict[str, float]
    sample_counts: dict[str, int]


@dataclass(frozen=True, slots=True)
class ReportAnalysis:
    record_count: int
    first_timestamp: datetime | None
    last_timestamp: datetime | None
    first_cycle: str | None
    last_cycle: str | None
    unique_cycles: int
    duplicate_cycles: int
    missing_cycles: int
    bad_part_count: int
    long_cycle_count: int
    profiles: dict[str, SignalProfile]
    event_counts: Counter[str]
    trends: dict[str, tuple[TrendRow, ...]]
    expected_signal_missing: tuple[str, ...]
    produced_parts: int | None = None
    rejected_parts: int | None = None
    sample_parts: int | None = None
    good_parts: int | None = None
    context_fields: tuple[str, ...] = ()

    @property
    def active_profiles(self) -> tuple[SignalProfile, ...]:
        return tuple(profile for profile in self.profiles.values() if not profile.is_clean_constant)

    @property
    def constant_profiles(self) -> tuple[SignalProfile, ...]:
        return tuple(profile for profile in self.profiles.values() if profile.is_clean_constant)

    @property
    def process_exception_count(self) -> int:
        return sum(
            count for name, count in self.event_counts.items() if name.startswith("Process:")
        )

    @property
    def data_quality_exception_count(self) -> int:
        return sum(
            count for name, count in self.event_counts.items() if name.startswith("Data Quality:")
        )


@dataclass(slots=True)
class _SignalAccumulator:
    signal_id: str
    display_name: str
    unit: str | None
    metadata: dict[str, object]
    rows: int = 0
    valid_readings: int = 0
    missing_readings: int = 0
    bad_quality_readings: int = 0
    uncertain_quality_readings: int = 0
    parse_failures: int = 0
    zero_count: int = 0
    low_limit_violations: int = 0
    high_limit_violations: int = 0
    first_value: object | None = None
    has_first_value: bool = False
    all_same: bool = True
    numeric_count: int = 0
    mean: float = 0.0
    m2: float = 0.0
    minimum: float | None = None
    maximum: float | None = None
    values: set[object] = field(default_factory=set)
    unique_values_truncated: bool = False
    qualities: Counter[str] = field(default_factory=Counter)

    def add(self, signal: SignalValue) -> None:
        self.rows += 1
        self.qualities[signal.quality.value] += 1
        if signal.quality is QualityStatus.BAD:
            self.bad_quality_readings += 1
            return
        if signal.quality is QualityStatus.UNCERTAIN:
            self.uncertain_quality_readings += 1
            return
        if signal.quality in {QualityStatus.PARSE_ERROR, QualityStatus.INVALID_NUMERIC}:
            self.parse_failures += 1
            return
        if signal.quality in {QualityStatus.MISSING, QualityStatus.STALE} or signal.value is None:
            self.missing_readings += 1
            return
        self.valid_readings += 1
        if not self.has_first_value:
            self.first_value = signal.value
            self.has_first_value = True
        elif signal.value != self.first_value:
            self.all_same = False
        if len(self.values) < _MAX_TRACKED_UNIQUE_VALUES:
            try:
                self.values.add(signal.value)
            except TypeError:
                self.values.add(repr(signal.value))
        else:
            self.unique_values_truncated = True
        if not _is_number(signal.value):
            return
        value = float(signal.value)
        self.numeric_count += 1
        delta = value - self.mean
        self.mean += delta / self.numeric_count
        self.m2 += delta * (value - self.mean)
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)
        if value == 0:
            self.zero_count += 1
        low_limit = _number_or_none(self.metadata.get("low_limit"))
        high_limit = _number_or_none(self.metadata.get("high_limit"))
        if low_limit is not None and value < low_limit:
            self.low_limit_violations += 1
        if high_limit is not None and value > high_limit:
            self.high_limit_violations += 1

    def profile(self, total_records: int) -> SignalProfile:
        absent_rows = total_records - self.rows
        valid = self.valid_readings
        missing = self.missing_readings + absent_rows
        variance = self.m2 / (self.numeric_count - 1) if self.numeric_count > 1 else 0.0
        clean_constant = (
            total_records > 0
            and self.rows == total_records
            and valid == total_records
            and self.all_same
            and len(self.values) == 1
            and self.bad_quality_readings == 0
            and self.uncertain_quality_readings == 0
            and self.parse_failures == 0
        )
        return SignalProfile(
            signal_id=self.signal_id,
            display_name=self.display_name,
            unit=self.unit,
            category=str(self.metadata.get("category", "miscellaneous")),
            metadata=self.metadata,
            rows=total_records,
            valid_readings=valid,
            missing_readings=missing,
            bad_quality_readings=self.bad_quality_readings,
            uncertain_quality_readings=self.uncertain_quality_readings,
            parse_failures=self.parse_failures,
            unique_values=len(self.values) + int(self.unique_values_truncated),
            minimum=self.minimum,
            average=self.mean if self.numeric_count else None,
            standard_deviation=math.sqrt(variance) if self.numeric_count else None,
            maximum=self.maximum,
            zero_count=self.zero_count,
            low_limit_violations=self.low_limit_violations,
            high_limit_violations=self.high_limit_violations,
            constant_value=self.first_value if clean_constant else None,
            is_clean_constant=clean_constant,
            quality_summary=", ".join(sorted(self.qualities)) or "Missing",
        )


@dataclass(slots=True)
class _TrendBucket:
    cycle_start: int | None = None
    cycle_end: int | None = None
    start_timestamp: datetime | None = None
    end_timestamp: datetime | None = None

    def add(self, record: ProcessRecord) -> None:
        timestamp = record.source_timestamp
        if timestamp is not None:
            self.start_timestamp = (
                timestamp if self.start_timestamp is None else min(self.start_timestamp, timestamp)
            )
            self.end_timestamp = (
                timestamp if self.end_timestamp is None else max(self.end_timestamp, timestamp)
            )
        cycle = _integer_or_none(record.cycle_id) if record.cycle_id else None
        if cycle is not None:
            self.cycle_start = cycle if self.cycle_start is None else min(self.cycle_start, cycle)
            self.cycle_end = cycle if self.cycle_end is None else max(self.cycle_end, cycle)


def analyse_records(
    records: RecordFactory,
    signal_registry: dict[str, dict[str, object]],
    timezone_name: str,
    trend_bucket_minutes: int,
    rules: dict[str, Any],
    metrics: PipelineMetrics | None = None,
    trend_bucket_count: int = 30,
) -> ReportAnalysis:
    """Analyze raw records in one bounded-memory pass before writing workbook sheets."""
    accumulators: dict[str, _SignalAccumulator] = {}
    event_counts: Counter[str] = Counter()
    trend_sums: dict[tuple[str, int, str], list[float]] = {}
    record_count = sum(1 for _ in records())
    trend_buckets: dict[int, _TrendBucket] = {}
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    first_cycle: str | None = None
    last_cycle: str | None = None
    timestamp_order, cycle_order = _detect_source_order(records)
    seen_cycles: set[tuple[str | None, str]] = set()
    seen_timestamps: set[datetime] = set()
    previous_cycle: int | None = None
    previous_run: str | None = None
    previous_timestamp: datetime | None = None
    duplicate_cycles = 0
    missing_cycles = 0
    bad_part_count = 0
    long_cycle_count = 0
    long_cycle_seconds = _long_cycle_seconds(rules)
    source_gap_seconds = _source_gap_seconds(rules)
    part_totals = PartTotals()
    context_fields: set[str] = set()

    scan_started = time.perf_counter()
    for source_index, record in enumerate(records()):
        part_totals.add(record, signal_registry)
        context_fields.update(
            key
            for key, value in record.production_context.items()
            if value is not None and key != "snapshot_complete"
        )
        if record.run_id is not None:
            context_fields.add("run_id")
        if record.run_id != previous_run:
            previous_cycle = None
            previous_run = record.run_id
        timestamp = record.source_timestamp
        trend_bucket = _trend_bucket_index(record.source_timestamp, trend_bucket_minutes)
        if trend_bucket is not None:
            trend_buckets.setdefault(trend_bucket, _TrendBucket()).add(record)
        if timestamp is None:
            event_counts["Data Quality:Missing timestamp"] += 1
        else:
            if first_timestamp is None or timestamp < first_timestamp:
                first_timestamp = timestamp
                first_cycle = record.cycle_id
            if last_timestamp is None or timestamp > last_timestamp:
                last_timestamp = timestamp
                last_cycle = record.cycle_id
            if timestamp in seen_timestamps:
                event_counts["Data Quality:Duplicate timestamp"] += 1
            seen_timestamps.add(timestamp)
            if (
                previous_timestamp is not None
                and (timestamp - previous_timestamp).total_seconds() * timestamp_order < 0
            ):
                event_counts["Data Quality:Backwards timestamp"] += 1
            if previous_timestamp is not None:
                elapsed = (timestamp - previous_timestamp).total_seconds() * timestamp_order
                if elapsed > source_gap_seconds:
                    event_counts["Data Quality:Source data gap"] += 1
            previous_timestamp = timestamp

        cycle = record.cycle_id
        if cycle:
            if first_cycle is None:
                first_cycle = cycle
            if last_cycle is None:
                last_cycle = cycle
            if (record.run_id, cycle) in seen_cycles:
                duplicate_cycles += 1
                event_counts["Data Quality:Duplicate cycle"] += 1
            seen_cycles.add((record.run_id, cycle))
            numeric_cycle = _integer_or_none(cycle)
            if numeric_cycle is not None and previous_cycle is not None:
                signed_delta = (numeric_cycle - previous_cycle) * cycle_order
                if signed_delta > 1:
                    missing_cycles += signed_delta - 1
                    event_counts["Data Quality:Cycle gap"] += 1
                elif signed_delta < 0:
                    event_counts["Data Quality:Cycle reset"] += 1
            if numeric_cycle is not None:
                previous_cycle = numeric_cycle

        for signal in record.signals:
            metadata = signal_registry.get(signal.signal_id, {})
            accumulator = accumulators.setdefault(
                signal.signal_id,
                _SignalAccumulator(signal.signal_id, signal.display_name, signal.unit, metadata),
            )
            accumulator.add(signal)
            for event in _signal_events(record, signal, metadata, long_cycle_seconds):
                event_counts[f"{event.category}:{event.event_type}"] += 1
                if event.event_type == "Bad part":
                    bad_part_count += 1
                elif event.event_type == "Long cycle":
                    long_cycle_count += 1
            if (
                trend_bucket is not None
                and _is_number(signal.value)
                and signal.quality is QualityStatus.GOOD
            ):
                category = str(metadata.get("category", "miscellaneous"))
                if bool(metadata.get("trend", False)):
                    values = trend_sums.setdefault(
                        (category, trend_bucket, signal.signal_id), [0.0, 0.0]
                    )
                    values[0] += float(signal.value)
                    values[1] += 1

    if metrics is not None:
        metrics.phases_seconds["analysis_scan"] += time.perf_counter() - scan_started
        metrics.observe_memory()
    if metrics is None:
        profiles = {
            signal_id: accumulator.profile(record_count)
            for signal_id, accumulator in accumulators.items()
        }
    else:
        with metrics.phase("constant_classification"):
            profiles = {
                signal_id: accumulator.profile(record_count)
                for signal_id, accumulator in accumulators.items()
            }
    expected_missing = tuple(
        signal_id
        for signal_id, metadata in signal_registry.items()
        if bool(metadata.get("required", False)) and signal_id not in profiles
    )
    event_counts["Data Quality:Expected signal missing"] += len(expected_missing)
    if metrics is None:
        trends = _build_trends(trend_sums, trend_buckets)
    else:
        with metrics.phase("trend_aggregation"):
            trends = _build_trends(trend_sums, trend_buckets)
    return ReportAnalysis(
        record_count=record_count,
        first_timestamp=first_timestamp,
        last_timestamp=last_timestamp,
        first_cycle=first_cycle,
        last_cycle=last_cycle,
        unique_cycles=len(seen_cycles),
        duplicate_cycles=duplicate_cycles,
        missing_cycles=missing_cycles,
        bad_part_count=bad_part_count,
        long_cycle_count=long_cycle_count,
        profiles=profiles,
        event_counts=event_counts,
        trends=trends,
        expected_signal_missing=expected_missing,
        produced_parts=part_totals.produced
        if record_count and part_totals.produced_known
        else None,
        rejected_parts=part_totals.rejected
        if record_count and part_totals.rejected_known
        else None,
        sample_parts=part_totals.samples if record_count and part_totals.samples_known else None,
        good_parts=part_totals.good if record_count else None,
        context_fields=tuple(
            key
            for key in (
                "run_id",
                "job_id",
                "product_id",
                "mould_id",
                "material_batch",
                "active_cavities",
                "sample_parts",
                "rejected_parts",
            )
            if key in context_fields
        ),
    )


def iter_events(
    records: RecordFactory,
    analysis: ReportAnalysis,
    rules: dict[str, Any],
) -> Iterator[Event]:
    """Regenerate events without retaining an unbounded event list in memory."""
    previous_cycle: int | None = None
    previous_run: str | None = None
    previous_timestamp: datetime | None = None
    timestamp_order, cycle_order = _detect_source_order(records)
    seen_cycles: set[tuple[str | None, str]] = set()
    seen_timestamps: set[datetime] = set()
    long_cycle_seconds = _long_cycle_seconds(rules)
    source_gap_seconds = _source_gap_seconds(rules)
    for record in records():
        if record.run_id != previous_run:
            previous_cycle = None
            previous_run = record.run_id
        timestamp = record.source_timestamp or record.received_timestamp
        if record.source_timestamp is None:
            yield _event(
                timestamp,
                record.cycle_id,
                "Data Quality",
                "Missing timestamp",
                None,
                None,
                None,
                "Warning",
                "Source timestamp was not supplied",
            )
        else:
            if record.source_timestamp in seen_timestamps:
                yield _event(
                    timestamp,
                    record.cycle_id,
                    "Data Quality",
                    "Duplicate timestamp",
                    None,
                    None,
                    None,
                    "Warning",
                    "Source timestamp has already occurred",
                )
            if (
                previous_timestamp is not None
                and (record.source_timestamp - previous_timestamp).total_seconds() * timestamp_order
                < 0
            ):
                yield _event(
                    timestamp,
                    record.cycle_id,
                    "Data Quality",
                    "Backwards timestamp",
                    None,
                    None,
                    None,
                    "Warning",
                    "Source timestamp moved backwards",
                )
            if previous_timestamp is not None:
                elapsed = (
                    record.source_timestamp - previous_timestamp
                ).total_seconds() * timestamp_order
                if elapsed > source_gap_seconds:
                    yield _event(
                        timestamp,
                        record.cycle_id,
                        "Data Quality",
                        "Source data gap",
                        None,
                        elapsed,
                        source_gap_seconds,
                        "Warning",
                        "Gap in OEM source records; this does not prove a logger communications outage",
                    )
            seen_timestamps.add(record.source_timestamp)
            previous_timestamp = record.source_timestamp
        if record.cycle_id:
            if (record.run_id, record.cycle_id) in seen_cycles:
                yield _event(
                    timestamp,
                    record.cycle_id,
                    "Data Quality",
                    "Duplicate cycle",
                    None,
                    record.cycle_id,
                    None,
                    "Warning",
                    "Cycle ID has already occurred",
                )
            seen_cycles.add((record.run_id, record.cycle_id))
            numeric_cycle = _integer_or_none(record.cycle_id)
            if numeric_cycle is not None and previous_cycle is not None:
                signed_delta = (numeric_cycle - previous_cycle) * cycle_order
                if signed_delta > 1:
                    low, high = sorted((previous_cycle, numeric_cycle))
                    detail = f"Missing cycle IDs {low + 1} to {high - 1}"
                    yield _event(
                        timestamp,
                        record.cycle_id,
                        "Data Quality",
                        "Cycle gap",
                        None,
                        numeric_cycle,
                        previous_cycle,
                        "Warning",
                        detail,
                    )
                elif signed_delta < 0:
                    yield _event(
                        timestamp,
                        record.cycle_id,
                        "Data Quality",
                        "Cycle reset",
                        None,
                        numeric_cycle,
                        previous_cycle,
                        "Warning",
                        "Cycle order reversed; no large gap inferred",
                    )
            if numeric_cycle is not None:
                previous_cycle = numeric_cycle
        for signal in record.signals:
            metadata = analysis.profiles[signal.signal_id].metadata
            yield from _signal_events(record, signal, metadata, long_cycle_seconds)
    source = "Configured signal did not occur in this report period"
    for signal_id in analysis.expected_signal_missing:
        yield _event(
            analysis.first_timestamp or datetime.now(UTC),
            None,
            "Data Quality",
            "Expected signal missing",
            signal_id,
            None,
            None,
            "Error",
            source,
        )


def _signal_events(
    record: ProcessRecord,
    signal: SignalValue,
    metadata: dict[str, object],
    long_cycle_seconds: float | None,
) -> Iterator[Event]:
    timestamp = record.source_timestamp or record.received_timestamp
    if signal.quality is QualityStatus.INVALID_NUMERIC:
        yield _event(
            timestamp,
            record.cycle_id,
            "Data Quality",
            "Source numeric overflow / invalid numeric value",
            signal.signal_id,
            signal.raw_value,
            None,
            "Warning",
            "Source numeric value could not be represented; preserved in the raw archive",
        )
        return
    if signal.quality is QualityStatus.PARSE_ERROR:
        yield _event(
            timestamp,
            record.cycle_id,
            "Data Quality",
            "Parse failure",
            signal.signal_id,
            None,
            None,
            "Warning",
            "Source value did not match expected datatype",
        )
        return
    if signal.quality in {QualityStatus.MISSING, QualityStatus.STALE} or signal.value is None:
        yield _event(
            timestamp,
            record.cycle_id,
            "Data Quality",
            "NULL reading",
            signal.signal_id,
            None,
            None,
            "Warning",
            "Signal value was missing",
        )
        return
    if signal.quality is QualityStatus.BAD:
        yield _event(
            timestamp,
            record.cycle_id,
            "Data Quality",
            "Bad quality",
            signal.signal_id,
            signal.value,
            None,
            "Warning",
            "Source marked value Bad",
        )
        return
    if signal.quality is QualityStatus.UNCERTAIN:
        yield _event(
            timestamp,
            record.cycle_id,
            "Data Quality",
            "Uncertain quality",
            signal.signal_id,
            signal.value,
            None,
            "Warning",
            "Source marked value Uncertain",
        )
        return
    if isinstance(signal.value, bool) and metadata.get("exception_rule") == "bad_part":
        if signal.value:
            yield _event(
                timestamp,
                record.cycle_id,
                "Process",
                "Bad part",
                signal.signal_id,
                True,
                False,
                "Warning",
                "Configured bad-cycle indicator is true; this does not establish a physical reject quantity",
            )
        return
    if not _is_number(signal.value):
        return
    value = float(signal.value)
    rule = str(metadata.get("exception_rule", ""))
    if rule == "bad_part" and value != 0:
        yield _event(
            timestamp,
            record.cycle_id,
            "Process",
            "Bad part",
            signal.signal_id,
            value,
            0,
            "Warning",
            "Configured bad-part indicator is non-zero",
        )
    if rule == "long_cycle" and long_cycle_seconds is not None and value > long_cycle_seconds:
        yield _event(
            timestamp,
            record.cycle_id,
            "Process",
            "Long cycle",
            signal.signal_id,
            value,
            long_cycle_seconds,
            "Warning",
            "Configured long-cycle threshold exceeded",
        )
    low_limit = _number_or_none(metadata.get("low_limit"))
    high_limit = _number_or_none(metadata.get("high_limit"))
    if low_limit is not None and value < low_limit:
        yield _event(
            timestamp,
            record.cycle_id,
            "Process",
            "Below low limit",
            signal.signal_id,
            value,
            low_limit,
            "Warning",
            "Configured low limit exceeded",
        )
    if high_limit is not None and value > high_limit:
        yield _event(
            timestamp,
            record.cycle_id,
            "Process",
            "Above high limit",
            signal.signal_id,
            value,
            high_limit,
            "Warning",
            "Configured high limit exceeded",
        )


def _event(
    timestamp: datetime,
    cycle_id: str | None,
    category: str,
    event_type: str,
    signal_id: str | None,
    value: object | None,
    threshold: object | None,
    severity: str,
    detail: str,
) -> Event:
    return Event(
        timestamp,
        category,
        event_type,
        severity,
        detail,
        cycle_id,
        signal_id,
        value,
        threshold,
        "edge-logger",
    )


def _build_trends(
    sums: dict[tuple[str, int, str], list[float]], buckets: dict[int, _TrendBucket]
) -> dict[str, tuple[TrendRow, ...]]:
    grouped: dict[str, dict[int, tuple[dict[str, float], dict[str, int]]]] = {}
    for (category, bucket, signal_id), (total, count) in sums.items():
        values, counts = grouped.setdefault(category, {}).setdefault(bucket, ({}, {}))
        values[signal_id] = total / count
        counts[signal_id] = int(count)
    return {
        category: tuple(
            TrendRow(
                interval=interval,
                cycle_start=buckets[bucket].cycle_start,
                cycle_end=buckets[bucket].cycle_end,
                start_timestamp=buckets[bucket].start_timestamp,
                end_timestamp=buckets[bucket].end_timestamp,
                values=values,
                sample_counts=counts,
            )
            for interval, (bucket, (values, counts)) in enumerate(sorted(rows.items()), start=1)
        )
        for category, rows in grouped.items()
    }


def _detect_source_order(records: RecordFactory, minimum_steps: int = 8) -> tuple[int, int]:
    """Detect a consistently newest-first export without treating it as corrupt data."""
    timestamp_direction = 0
    cycle_direction = 0
    timestamp_steps = 0
    cycle_steps = 0
    timestamp_consistent = True
    cycle_consistent = True
    previous_timestamp: datetime | None = None
    previous_cycle: int | None = None
    for index, record in enumerate(records()):
        if index >= 32:
            break
        if record.source_timestamp is not None and previous_timestamp is not None:
            step = _timestamp_direction(record.source_timestamp, previous_timestamp)
            if step:
                if timestamp_direction and timestamp_direction != step:
                    timestamp_consistent = False
                elif timestamp_consistent:
                    timestamp_direction = step
                    timestamp_steps += 1
        if record.source_timestamp is not None:
            previous_timestamp = record.source_timestamp
        numeric_cycle = _integer_or_none(record.cycle_id) if record.cycle_id else None
        if numeric_cycle is not None and previous_cycle is not None:
            step = _integer_direction(numeric_cycle, previous_cycle)
            if step:
                if cycle_direction and cycle_direction != step:
                    cycle_consistent = False
                elif cycle_consistent:
                    cycle_direction = step
                    cycle_steps += 1
        if numeric_cycle is not None:
            previous_cycle = numeric_cycle
    return (
        timestamp_direction if timestamp_consistent and timestamp_steps >= minimum_steps else 1,
        cycle_direction if cycle_consistent and cycle_steps >= minimum_steps else 1,
    )


def _timestamp_direction(current: datetime, previous: datetime) -> int:
    if current > previous:
        return 1
    if current < previous:
        return -1
    return 0


def _integer_direction(current: int, previous: int) -> int:
    if current > previous:
        return 1
    if current < previous:
        return -1
    return 0


def _trend_bucket_index(timestamp: datetime | None, trend_bucket_minutes: int) -> int | None:
    if timestamp is None:
        return None
    seconds = trend_bucket_minutes * 60
    return int(timestamp.timestamp()) // seconds


def _long_cycle_seconds(rules: dict[str, Any]) -> float | None:
    value = rules.get("long_cycle_seconds")
    if value is None:
        return None
    return float(value)


def _source_gap_seconds(rules: dict[str, Any]) -> float:
    value = rules.get("source_data_gap_seconds", 60.0)
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        return 60.0
    return float(value)


def _is_number(value: object | None) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _number_or_none(value: object | None) -> float | None:
    if not isinstance(value, (int, float, str)) or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _integer_or_none(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None
