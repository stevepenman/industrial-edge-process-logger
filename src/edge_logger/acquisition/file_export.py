"""Streaming import of DEMO-01 UTF-16 tab-separated OEM exports.

The immutable source archive retains every byte. This parser separately
normalises headers for registry lookup, recognises an engineering-units row,
and ignores only a wholly blank trailing structural column.
"""

from __future__ import annotations

import csv
import hashlib
import math
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import chain, pairwise
from pathlib import Path
from zoneinfo import ZoneInfo

from ..models import ProcessRecord, QualityStatus, SignalValue
from ..storage.source_archive import sha256_file

_TIMESTAMP_HEADERS = ("timestamp", "date time", "datetime", "time stamp")
_DATE_HEADERS = ("date", "datum")
_TIME_HEADERS = ("time", "zeit")
_CYCLE_HEADERS = ("cycle", "cycle id", "cycle number", "cycles")
_INVALID_NUMERIC_MARKERS = {"OVERRANGE"}


@dataclass(frozen=True, slots=True)
class SourceInspection:
    encoding: str
    delimiter: str
    row_count: int
    columns: tuple[str, ...]
    units: tuple[str, ...]
    timestamp_column: str | None
    date_column: str | None
    time_column: str | None
    cycle_column: str | None
    known_signals: tuple[str, ...]
    unknown_signals: tuple[str, ...]
    parse_issue_count: int
    physical_column_count: int
    named_column_count: int
    ignored_blank_trailing_columns: int
    source_ordering: str


@dataclass(frozen=True, slots=True)
class _Schema:
    timestamp_column: str | None
    date_column: str | None
    time_column: str | None
    cycle_column: str | None


@dataclass(frozen=True, slots=True)
class _Layout:
    physical_headers: tuple[str, ...]
    columns: tuple[str, ...]
    units_by_column: dict[str, str]
    schema: _Schema
    ignored_blank_trailing_columns: int
    first_data_row: list[str] | None


class DuplicateSourceError(ValueError):
    """The exact source content was already imported."""


class FileExportImporter:
    """Map every named source column to a canonical signal without dropping unknowns."""

    def __init__(
        self,
        machine_name: str,
        signal_registry: dict[str, dict[str, object]],
        timezone_name: str = "Europe/London",
    ) -> None:
        self.machine_name = machine_name
        self.signal_registry = signal_registry
        self.timezone = ZoneInfo(timezone_name)
        self._registry_by_source = self._registry_lookup(signal_registry)

    def checksum(self, path: Path) -> str:
        return sha256_file(path)

    def inspect(self, path: Path) -> SourceInspection:
        with path.open("r", encoding="utf-16", newline="") as source:
            reader = csv.reader(source, delimiter="\t")
            layout = _read_layout(reader)
            parse_issue_count = 0
            row_count = 0
            timestamps: list[datetime] = []
            for row in chain(_first_row(layout), reader):
                if not any(cell.strip() for cell in row):
                    continue
                row_count += 1
                values = _row_values(layout.columns, row)
                timestamp = _parse_source_timestamp(values, layout.schema, self.timezone)
                if timestamp is not None and len(timestamps) < 32:
                    timestamps.append(timestamp)
                for column in _signal_columns(layout.columns, layout.schema):
                    metadata = self._metadata(column)
                    _, quality = _parse_value(values.get(column), metadata.get("expected_datatype"))
                    if quality in {QualityStatus.PARSE_ERROR, QualityStatus.INVALID_NUMERIC}:
                        parse_issue_count += 1
            signal_columns = _signal_columns(layout.columns, layout.schema)
            known = tuple(column for column in signal_columns if self._metadata(column))
            unknown = tuple(column for column in signal_columns if not self._metadata(column))
            return SourceInspection(
                encoding="UTF-16 little-endian",
                delimiter="tab",
                row_count=row_count,
                columns=layout.columns,
                units=tuple(layout.units_by_column.get(column, "") for column in layout.columns),
                timestamp_column=layout.schema.timestamp_column,
                date_column=layout.schema.date_column,
                time_column=layout.schema.time_column,
                cycle_column=layout.schema.cycle_column,
                known_signals=known,
                unknown_signals=unknown,
                parse_issue_count=parse_issue_count,
                physical_column_count=len(layout.physical_headers),
                named_column_count=len(layout.columns),
                ignored_blank_trailing_columns=layout.ignored_blank_trailing_columns,
                source_ordering=_source_ordering(timestamps),
            )

    def records(self, path: Path) -> Iterator[ProcessRecord]:
        with path.open("r", encoding="utf-16", newline="") as source:
            reader = csv.reader(source, delimiter="\t")
            layout = _read_layout(reader)
            start_row = 3 if layout.first_data_row is None else 2
            for row_number, row in enumerate(chain(_first_row(layout), reader), start=start_row):
                if not any(cell.strip() for cell in row):
                    continue
                yield self._record(_row_values(layout.columns, row), path.name, row_number, layout)

    def _record(
        self,
        row: dict[str, str | None],
        source_name: str,
        row_number: int,
        layout: _Layout,
    ) -> ProcessRecord:
        source_timestamp = _parse_source_timestamp(row, layout.schema, self.timezone)
        cycle_id = row.get(layout.schema.cycle_column) if layout.schema.cycle_column else None
        signals: list[SignalValue] = []
        for source_column in _signal_columns(layout.columns, layout.schema):
            metadata = self._metadata(source_column)
            raw_value = row.get(source_column)
            value, quality = _parse_value(raw_value, metadata.get("expected_datatype"))
            signals.append(
                SignalValue(
                    signal_id=str(metadata.get("id", _stable_id(source_column))),
                    display_name=str(metadata.get("display_name", source_column.strip())),
                    value=value,
                    unit=_optional_string(metadata.get("unit"))
                    or layout.units_by_column.get(source_column)
                    or None,
                    datatype=_optional_string(metadata.get("expected_datatype"))
                    or _datatype(value),
                    quality=quality,
                    source_timestamp=source_timestamp,
                    source_name=source_column,
                    raw_value=raw_value,
                )
            )
        return ProcessRecord(
            machine_name=self.machine_name,
            source_timestamp=source_timestamp,
            received_timestamp=datetime.now(UTC),
            cycle_id=cycle_id.strip() if cycle_id and cycle_id.strip() else None,
            source_identifier=f"{source_name}:{row_number}",
            signals=tuple(signals),
        )

    def _metadata(self, source_column: str) -> dict[str, object]:
        return self._registry_by_source.get(_normalise(source_column), {})

    @staticmethod
    def _registry_lookup(registry: dict[str, dict[str, object]]) -> dict[str, dict[str, object]]:
        lookup: dict[str, dict[str, object]] = {}
        for configured_id, metadata in registry.items():
            configured = {"id": configured_id, **metadata}
            names = (
                configured_id,
                metadata.get("source_name"),
                metadata.get("oem_name"),
                metadata.get("display_name"),
            )
            for name in names:
                if isinstance(name, str) and name.strip():
                    lookup[_normalise(name)] = configured
        return lookup


def _read_layout(reader: Iterator[list[str]]) -> _Layout:
    try:
        physical_headers: tuple[str, ...] = tuple(next(reader))
    except StopIteration as error:
        raise ValueError("DEMO-01 export has no header row") from error
    if not any(header.strip() for header in physical_headers):
        raise ValueError("DEMO-01 export has no named header row")
    ignored = 0
    while (
        len(physical_headers) - ignored > 0
        and not physical_headers[len(physical_headers) - ignored - 1].strip()
    ):
        ignored += 1
    columns = physical_headers[: len(physical_headers) - ignored] if ignored else physical_headers
    normalised = tuple(_normalise(column) for column in columns)
    if any(not column for column in normalised) or len(set(normalised)) != len(normalised):
        raise ValueError("Source headers contain blank or duplicate named columns")
    schema = _schema(columns)
    try:
        next_row: list[str] = next(reader)
    except StopIteration:
        next_row = []
    units_by_column: dict[str, str] = {}
    first_data_row: list[str] | None = next_row
    if _is_units_row(next_row, schema, columns):
        first_data_row = None
        for index, column in enumerate(columns):
            units_by_column[column] = next_row[index].strip() if index < len(next_row) else ""
    return _Layout(physical_headers, columns, units_by_column, schema, ignored, first_data_row)


def _first_row(layout: _Layout) -> Iterator[list[str]]:
    if layout.first_data_row is not None:
        yield layout.first_data_row


def _is_units_row(row: list[str], schema: _Schema, columns: tuple[str, ...]) -> bool:
    values = _row_values(columns, row)
    timestamp = _parse_source_timestamp(values, schema, ZoneInfo("UTC"))
    cycle = values.get(schema.cycle_column) if schema.cycle_column else None
    return (
        timestamp is None
        and (cycle is None or not cycle.strip())
        and any(value.strip() for value in row)
    )


def _row_values(columns: tuple[str, ...], row: list[str]) -> dict[str, str | None]:
    if any(value.strip() for value in row[len(columns) :]):
        raise ValueError("Source row contains nonblank values in unnamed or extra columns")
    return {
        column: row[index] if index < len(row) else None for index, column in enumerate(columns)
    }


def _schema(fieldnames: tuple[str, ...]) -> _Schema:
    by_normalised = {_normalise(column): column for column in fieldnames}
    timestamp = next(
        (by_normalised[name] for name in _TIMESTAMP_HEADERS if name in by_normalised), None
    )
    date = next((by_normalised[name] for name in _DATE_HEADERS if name in by_normalised), None)
    time = next((by_normalised[name] for name in _TIME_HEADERS if name in by_normalised), None)
    cycle = next((by_normalised[name] for name in _CYCLE_HEADERS if name in by_normalised), None)
    return _Schema(timestamp, date, time, cycle)


def _signal_columns(fieldnames: tuple[str, ...], schema: _Schema) -> tuple[str, ...]:
    metadata = {
        schema.timestamp_column,
        schema.date_column,
        schema.time_column,
        schema.cycle_column,
    }
    return tuple(column for column in fieldnames if column not in metadata)


def _normalise(value: str | None) -> str:
    return " ".join((value or "").strip().lower().replace("_", " ").split())


def _stable_id(value: str) -> str:
    label = "".join(character if character.isalnum() else "_" for character in value.lower()).strip(
        "_"
    )
    return f"unknown_{label}_{hashlib.sha256(value.encode()).hexdigest()[:12]}"


def _parse_value(
    raw_value: str | None, expected_datatype: object | None
) -> tuple[object | None, QualityStatus]:
    if raw_value is None or not raw_value.strip():
        return None, QualityStatus.MISSING
    value = raw_value.strip()
    datatype = str(expected_datatype).lower()
    if datatype in {"str", "string"}:
        return value, QualityStatus.GOOD
    if datatype in {"bool", "boolean"}:
        if value.lower() in {"true", "1", "false", "0"}:
            return value.lower() in {"true", "1"}, QualityStatus.GOOD
        return None, QualityStatus.PARSE_ERROR
    expects_numeric = str(expected_datatype).lower() in {"float", "int", "number", "numeric"}
    if expects_numeric and value in _INVALID_NUMERIC_MARKERS:
        return None, QualityStatus.INVALID_NUMERIC
    numeric = _parse_decimal(value)
    if numeric is not None:
        if not math.isfinite(numeric):
            return None, QualityStatus.INVALID_NUMERIC
        if datatype == "int":
            return (
                (int(numeric), QualityStatus.GOOD)
                if numeric.is_integer()
                else (None, QualityStatus.PARSE_ERROR)
            )
        return numeric, QualityStatus.GOOD
    if expects_numeric:
        return None, QualityStatus.PARSE_ERROR
    return value, QualityStatus.GOOD


def _parse_decimal(value: str) -> float | None:
    try:
        if "," in value and "." in value:
            normalised = (
                value.replace(".", "").replace(",", ".")
                if value.rfind(",") > value.rfind(".")
                else value.replace(",", "")
            )
        elif "," in value:
            normalised = value.replace(",", ".")
        else:
            normalised = value
        return float(normalised)
    except ValueError:
        return None


def _parse_source_timestamp(
    row: dict[str, str | None], schema: _Schema, timezone: ZoneInfo
) -> datetime | None:
    value = row.get(schema.timestamp_column) if schema.timestamp_column else None
    if value is None and schema.date_column and schema.time_column:
        value = f"{row.get(schema.date_column) or ''} {row.get(schema.time_column) or ''}".strip()
    if value is None or not value.strip():
        return None
    text = value.strip()
    for candidate in (text, text.replace(",", ".")):
        parsed: datetime | None
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            parsed = _parse_datetime_pattern(candidate, timezone)
        if parsed is not None:
            if parsed.tzinfo is not None:
                return parsed
            first = parsed.replace(tzinfo=timezone, fold=0)
            second = parsed.replace(tzinfo=timezone, fold=1)
            return first if first.utcoffset() == second.utcoffset() else None
    return None


def _parse_datetime_pattern(value: str, timezone: ZoneInfo) -> datetime | None:
    for pattern in ("%d.%m.%y %H:%M:%S", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, pattern)  # noqa: DTZ007 - localized only after rejecting ambiguous DST instants
        except ValueError:
            continue
    return None


def _source_ordering(timestamps: list[datetime]) -> str:
    steps = [later > earlier for earlier, later in pairwise(timestamps) if later != earlier]
    if len(steps) >= 8 and all(steps):
        return "ascending"
    if len(steps) >= 8 and not any(steps):
        return "descending"
    return "indeterminate"


def _datatype(value: object | None) -> str | None:
    return type(value).__name__ if value is not None else None


def _optional_string(value: object | None) -> str | None:
    return str(value) if value is not None else None
