"""Transactional, multi-pass production workbook generation."""

from __future__ import annotations

import hashlib
import math
import os
from collections.abc import Iterable, Iterator, Sequence
from contextlib import AbstractContextManager, nullcontext
from copy import copy
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import chain
from pathlib import Path
from shutil import copyfileobj
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile
from zoneinfo import ZoneInfo

import xlsxwriter  # type: ignore[import-untyped]

from ..analysis.pipeline import (
    RecordFactory,
    ReportAnalysis,
    SignalProfile,
    analyse_records,
    iter_events,
)
from ..metrics import PipelineMetrics
from ..models import ProcessRecord
from ..storage.raw_archive import RawArchiveWriter, read_records
from .charts import apply_line_chart_style, series_colour
from .styles import (
    COLOUR_AMBER,
    COLOUR_BLUE,
    COLOUR_MID_GREY,
    COLOUR_NAVY,
    ReportFormats,
    configure_sheet,
    create_report_formats,
)
from .validator import validate_workbook


class ReportGenerationError(RuntimeError):
    """A report cannot be promoted to an email-eligible workbook."""


@dataclass(frozen=True, slots=True)
class _TemperatureTrendLayout:
    sheet: xlsxwriter.worksheet.Worksheet
    profiles: tuple[SignalProfile, ...]
    row_count: int
    data_columns: dict[str, int]
    header_row: int
    data_start_row: int


def generate_report(
    records: Iterable[ProcessRecord] | RecordFactory,
    output_directory: Path,
    report_id: str,
    machine_name: str,
    timezone_name: str,
    source_manifest: dict[str, object],
    signal_registry: dict[str, dict[str, object]] | None = None,
    rules: dict[str, Any] | None = None,
    trend_bucket_minutes: int = 5,
    metrics: PipelineMetrics | None = None,
    trend_bucket_count: int = 30,
) -> tuple[Path, str]:
    """Generate a report without materialising a report period in memory."""
    output_directory.mkdir(parents=True, exist_ok=True)
    owned_archive: Path | None = None
    if callable(records):
        record_factory = records
    elif isinstance(records, Sequence):
        record_factory = lambda: iter(records)
    else:
        with RawArchiveWriter(output_directory, f".{report_id}.input") as archive:
            for record in records:
                archive.append(record)
        owned_archive = archive.path
        record_factory = lambda: read_records(owned_archive)
    try:
        return _generate(
            record_factory,
            output_directory,
            report_id,
            machine_name,
            timezone_name,
            source_manifest,
            signal_registry or {},
            rules or {},
            trend_bucket_minutes,
            metrics,
            trend_bucket_count,
        )
    finally:
        if owned_archive is not None and owned_archive.exists():
            owned_archive.unlink()


def generate_report_from_archives(
    archive_paths: Sequence[Path],
    output_directory: Path,
    report_id: str,
    machine_name: str,
    timezone_name: str,
    source_manifest: dict[str, object],
    signal_registry: dict[str, dict[str, object]],
    rules: dict[str, Any],
    trend_bucket_minutes: int,
    metrics: PipelineMetrics | None = None,
    trend_bucket_count: int = 30,
) -> tuple[Path, str]:
    """Use retained raw archives as a rewindable source for report regeneration."""
    paths = tuple(archive_paths)

    def record_factory() -> Iterator[ProcessRecord]:
        return chain.from_iterable(read_records(path) for path in paths)

    return _generate(
        record_factory,
        output_directory,
        report_id,
        machine_name,
        timezone_name,
        source_manifest,
        signal_registry,
        rules,
        trend_bucket_minutes,
        metrics,
        trend_bucket_count,
    )


def _generate(
    records: RecordFactory,
    output_directory: Path,
    report_id: str,
    machine_name: str,
    timezone_name: str,
    source_manifest: dict[str, object],
    signal_registry: dict[str, dict[str, object]],
    rules: dict[str, Any],
    trend_bucket_minutes: int,
    metrics: PipelineMetrics | None,
    trend_bucket_count: int,
) -> tuple[Path, str]:
    output_directory.mkdir(parents=True, exist_ok=True)
    analysis = analyse_records(
        records,
        signal_registry,
        timezone_name,
        trend_bucket_minutes,
        rules,
        metrics,
        trend_bucket_count,
    )
    destination = output_directory / _filename(machine_name, report_id)
    temporary = output_directory / f"{report_id}.tmp.xlsx"
    if temporary.exists():
        temporary.unlink()
    _write_workbook(
        temporary,
        records,
        analysis,
        report_id,
        machine_name,
        timezone_name,
        source_manifest,
        rules,
        metrics,
    )
    with _phase(metrics, "workbook_compression"):
        _recompress_workbook(temporary)
    with _phase(metrics, "openpyxl_validation"):
        validation = validate_workbook(temporary, report_id, analysis)
    if not validation.valid:
        raise ReportGenerationError("; ".join(validation.errors))
    _fsync_file(temporary)
    with _phase(metrics, "workbook_checksum"):
        checksum = _sha256(temporary)
    os.replace(temporary, destination)
    _fsync_directory(output_directory)
    return destination, checksum


def _write_workbook(
    path: Path,
    records: RecordFactory,
    analysis: ReportAnalysis,
    report_id: str,
    machine_name: str,
    timezone_name: str,
    source_manifest: dict[str, object],
    rules: dict[str, Any],
    metrics: PipelineMetrics | None,
) -> None:
    workbook = xlsxwriter.Workbook(
        path, {"constant_memory": True, "strings_to_formulas": False, "strings_to_urls": False}
    )
    formats = create_report_formats(workbook)
    timezone = ZoneInfo(timezone_name)
    try:
        with _phase(metrics, "other_worksheet_writing"):
            _write_summary(
                workbook.add_worksheet("Summary"),
                analysis,
                report_id,
                machine_name,
                timezone,
                source_manifest,
                formats,
            )
            _write_temperature_summary(
                workbook.add_worksheet("Temperature Summary"), analysis, report_id, formats
            )
            temperature_layout = _write_trends(
                workbook,
                "Temperature Trends",
                "temperature",
                analysis,
                report_id,
                formats,
                "TEMPERATURE TRENDS",
                timezone,
            )
            if temperature_layout is not None:
                with _phase(metrics, "chart_creation"):
                    _write_chart_tabs(
                        workbook,
                        temperature_layout,
                        "temperature",
                        analysis,
                        report_id,
                        timezone,
                        machine_name,
                        formats,
                    )
            process_layout = _write_trends(
                workbook,
                "Process Trends",
                "process",
                analysis,
                report_id,
                formats,
                "PROCESS TRENDS",
                timezone,
            )
            if process_layout is not None:
                with _phase(metrics, "chart_creation"):
                    _write_chart_tabs(
                        workbook,
                        process_layout,
                        "process",
                        analysis,
                        report_id,
                        timezone,
                        machine_name,
                        formats,
                    )
            _write_events(
                workbook.add_worksheet("Events - Exceptions"),
                records,
                analysis,
                rules,
                timezone,
                report_id,
                formats,
            )
        with _phase(metrics, "xlsx_active_raw_data"):
            _write_raw_data(
                workbook.add_worksheet("Active Raw Data"),
                records,
                analysis,
                timezone,
                report_id,
                formats,
            )
        with _phase(metrics, "other_worksheet_writing"):
            _write_constant_signals(
                workbook.add_worksheet("Constant Signals"), analysis, report_id, formats
            )
            _write_manifest(
                workbook.add_worksheet("Source Manifest"),
                analysis,
                report_id,
                machine_name,
                source_manifest,
                formats,
            )
    finally:
        workbook.close()


def _recompress_workbook(path: Path) -> None:
    """Apply maximum ZIP compression without materialising the raw-sheet XML."""

    compressed = path.with_name(f".{path.stem}.recompressed{path.suffix}")
    try:
        with (
            ZipFile(path) as source,
            ZipFile(
                compressed, "w", compression=ZIP_DEFLATED, compresslevel=9, allowZip64=True
            ) as destination,
        ):
            for member in source.infolist():
                output_member = copy(member)
                output_member.compress_type = ZIP_DEFLATED
                output_member._compresslevel = 9  # type: ignore[attr-defined]
                with (
                    source.open(member) as input_stream,
                    destination.open(output_member, "w", force_zip64=True) as output_stream,
                ):
                    copyfileobj(input_stream, output_stream, length=1024 * 1024)
        os.replace(compressed, path)
    finally:
        compressed.unlink(missing_ok=True)


def _write_summary(
    sheet: xlsxwriter.worksheet.Worksheet,
    analysis: ReportAnalysis,
    report_id: str,
    machine_name: str,
    timezone: ZoneInfo,
    source_manifest: dict[str, object],
    formats: ReportFormats,
) -> None:
    configure_sheet(sheet, COLOUR_NAVY, report_id, landscape=False, zoom=100)
    sheet.activate()
    sheet.set_column("A:A", 33)
    sheet.set_column("B:B", 25)
    # The lower Temperature Overview uses C and F for numeric values and status,
    # so they cannot remain the narrow spacers used by the summary blocks above.
    sheet.set_column("C:C", 13)
    sheet.set_column("D:D", 33)
    sheet.set_column("E:E", 25)
    sheet.set_column("F:F", 22)
    sheet.set_column("G:H", 17)
    sheet.set_row(0, 31)
    sheet.merge_range("A1:H1", f"{machine_name.upper()} PROCESS REPORT", formats.title)
    sheet.merge_range(
        "A2:H2",
        "Automatically generated DEMO-01 process monitoring report | Read-only monitoring system",
        formats.subtitle,
    )
    wall_seconds = _wall_seconds(analysis.first_timestamp, analysis.last_timestamp)
    report_information = [
        ("Machine", machine_name, None),
        ("Report ID", report_id, None),
        ("Report period start", _local(analysis.first_timestamp, timezone), formats.timestamp),
        ("Report period end", _local(analysis.last_timestamp, timezone), formats.timestamp),
        (
            "Generated timestamp",
            datetime.now(UTC).astimezone(timezone).replace(tzinfo=None),
            formats.timestamp,
        ),
        ("Application version", source_manifest.get("application_version", "0.1.0"), None),
        ("Configuration version", source_manifest.get("configuration_version"), None),
    ]
    production_summary = [
        ("Acquisition hours", source_manifest.get("acquisition_hours"), formats.decimal_1),
        (
            "Wall-clock period",
            wall_seconds / 3600 if wall_seconds is not None else None,
            formats.decimal_1,
        ),
        ("Raw record count", analysis.record_count, formats.integer),
        ("First cycle", analysis.first_cycle, None),
        ("Last cycle", analysis.last_cycle, None),
        ("Unique cycles", analysis.unique_cycles, formats.integer),
        ("Active signals", len(analysis.active_profiles), formats.integer),
        ("Constant signals", len(analysis.constant_profiles), formats.integer),
    ]
    quality_summary = [
        ("Duplicate cycles", analysis.duplicate_cycles, formats.integer),
        ("Missing cycles", analysis.missing_cycles, formats.integer),
        ("Cycles flagged bad", analysis.bad_part_count, formats.integer),
        (
            "Cycles flagged bad %",
            _ratio(analysis.bad_part_count, analysis.record_count),
            formats.percentage,
        ),
        ("Long cycles", analysis.long_cycle_count, formats.integer),
        ("Process exceptions", analysis.process_exception_count, formats.integer),
        ("Data-quality exceptions", analysis.data_quality_exception_count, formats.integer),
        ("Communication outages", source_manifest.get("communication_outages"), formats.integer),
        ("Physical parts produced", analysis.produced_parts, formats.integer),
        ("Physical parts rejected", analysis.rejected_parts, formats.integer),
        ("Sample parts", analysis.sample_parts, formats.integer),
        ("Good parts", analysis.good_parts, formats.integer),
    ]
    energy_summary = [
        (
            "Average drive energy per cycle (Wh)",
            _profile_average(analysis, "energy_cycle_drives_wh"),
            formats.decimal_1,
        ),
        (
            "Average heating energy per cycle (Wh)",
            _profile_average(analysis, "energy_cycle_heating_wh"),
            formats.decimal_1,
        ),
        (
            "Average total energy per cycle (Wh)",
            _profile_average(analysis, "energy_cycle_total_wh"),
            formats.decimal_1,
        ),
        (
            "Average drive energy (kWh/kg)",
            _profile_average(analysis, "energy_per_kg_drives"),
            formats.decimal_3,
        ),
        (
            "Average heating energy (kWh/kg)",
            _profile_average(analysis, "energy_per_kg_heating"),
            formats.decimal_3,
        ),
        (
            "Average total energy (kWh/kg)",
            _profile_average(analysis, "energy_per_kg_total"),
            formats.decimal_3,
        ),
        ("Energy data-quality errors", _energy_error_count(analysis), formats.integer),
    ]
    _write_parallel_summary_blocks(
        sheet,
        4,
        "Report Information",
        report_information,
        "Production Summary",
        production_summary,
        formats,
    )
    quality_start = 15
    # In constant-memory mode a row cannot be revisited. Write these two
    # side-by-side sections together so the Energy Performance block is not
    # discarded after the Quality / Exceptions cells have been written.
    _write_parallel_summary_blocks(
        sheet,
        quality_start,
        "Quality / Exceptions",
        quality_summary,
        "Energy Performance",
        energy_summary,
        formats,
    )
    for offset, (_, value, _) in enumerate(quality_summary, start=1):
        row = quality_start + offset
        sheet.conditional_format(
            row,
            1,
            row,
            1,
            {"type": "cell", "criteria": "==", "value": 0, "format": formats.status_normal},
        )
        sheet.conditional_format(
            row,
            1,
            row,
            1,
            {"type": "cell", "criteria": ">", "value": 0, "format": formats.status_warning},
        )

    overview_start = quality_start + len(quality_summary) + 3
    sheet.merge_range(
        overview_start, 0, overview_start, 5, "Temperature Overview", formats.section_header
    )
    headers = ["Signal", "Unit", "Minimum", "Average", "Maximum", "Status"]
    _table_header(sheet, overview_start + 1, headers, formats)
    profiles = _profiles(analysis, "temperature")
    for row, profile in enumerate(profiles, start=overview_start + 2):
        _write_row(
            sheet,
            row,
            [
                profile.display_name,
                _display_unit(profile.unit),
                profile.minimum,
                profile.average,
                profile.maximum,
                profile.status,
            ],
            [
                formats.table_text,
                formats.table_text,
                formats.table_decimal_1,
                formats.table_decimal_1,
                formats.table_decimal_1,
                formats.table_text,
            ],
        )
    if profiles:
        _apply_banding(sheet, overview_start + 2, 0, overview_start + 1 + len(profiles), 5, formats)
        _apply_status_formatting(
            sheet, overview_start + 2, 5, overview_start + 1 + len(profiles), formats
        )
    sheet.print_area(0, 0, overview_start + 1 + len(profiles), 7)


def _write_temperature_summary(
    sheet: xlsxwriter.worksheet.Worksheet,
    analysis: ReportAnalysis,
    report_id: str,
    formats: ReportFormats,
) -> None:
    headers = [
        "Signal",
        "Unit",
        "Records",
        "Valid readings",
        "Missing/NULL",
        "Unique values",
        "Minimum",
        "Average",
        "Std. dev.",
        "Maximum",
        "Range/span",
        "Zero count",
        "Low limit",
        "High limit",
        "Low violations",
        "High violations",
        "Status",
    ]
    configure_sheet(
        sheet, COLOUR_BLUE, report_id, landscape=True, zoom=90, repeat_header_rows=(3, 3)
    )
    sheet.set_row(0, 31)
    sheet.merge_range(0, 0, 0, len(headers) - 1, "TEMPERATURE SUMMARY", formats.title)
    sheet.merge_range(
        1,
        0,
        1,
        len(headers) - 1,
        "Statistical overview of configured temperature signals",
        formats.subtitle,
    )
    _table_header(sheet, 3, headers, formats)
    profiles = _profiles(analysis, "temperature")
    for row, profile in enumerate(profiles, start=4):
        span = _span(profile)
        values = [
            profile.display_name,
            _display_unit(profile.unit),
            profile.rows,
            profile.valid_readings,
            profile.missing_readings,
            profile.unique_values,
            profile.minimum,
            profile.average,
            profile.standard_deviation,
            profile.maximum,
            span,
            profile.zero_count,
            profile.metadata.get("low_limit"),
            profile.metadata.get("high_limit"),
            profile.low_limit_violations,
            profile.high_limit_violations,
            profile.status,
        ]
        _write_row(
            sheet,
            row,
            values,
            [
                formats.table_text,
                formats.table_text,
                formats.table_integer,
                formats.table_integer,
                formats.table_integer,
                formats.table_integer,
                formats.table_decimal_1,
                formats.table_decimal_1,
                formats.table_decimal_1,
                formats.table_decimal_1,
                formats.table_decimal_1,
                formats.table_integer,
                formats.table_decimal_1,
                formats.table_decimal_1,
                formats.table_integer,
                formats.table_integer,
                formats.table_text,
            ],
        )
    last_row = 3 + len(profiles)
    sheet.freeze_panes(4, 0)
    sheet.autofilter(3, 0, last_row, len(headers) - 1)
    _set_column_widths(
        sheet,
        (34, 10, 12, 16, 14, 15, 13, 13, 13, 13, 13, 12, 13, 13, 16, 16, 20),
    )
    sheet.set_row(3, 30)
    if profiles:
        _apply_banding(sheet, 4, 0, last_row, len(headers) - 1, formats)
        _apply_status_formatting(sheet, 4, len(headers) - 1, last_row, formats)
        _apply_nonzero_formatting(sheet, 4, (4, 14, 15), last_row, formats)
    sheet.print_area(0, 0, last_row, len(headers) - 1)


def _write_constant_signals(
    sheet: xlsxwriter.worksheet.Worksheet,
    analysis: ReportAnalysis,
    report_id: str,
    formats: ReportFormats,
) -> None:
    headers = [
        "Signal",
        "OEM source field",
        "Category",
        "Unit",
        "Constant value",
        "Records",
        "Status",
    ]
    configure_sheet(
        sheet, COLOUR_MID_GREY, report_id, landscape=True, zoom=90, repeat_header_rows=(3, 3)
    )
    sheet.set_row(0, 31)
    sheet.merge_range(0, 0, 0, len(headers) - 1, "CONSTANT SIGNALS", formats.title)
    sheet.merge_range(
        1,
        0,
        1,
        len(headers) - 1,
        "Clean constants are classified for this report period only",
        formats.subtitle,
    )
    _table_header(sheet, 3, headers, formats)
    profiles = analysis.constant_profiles
    for row, profile in enumerate(profiles, start=4):
        _write_row(
            sheet,
            row,
            [
                profile.display_name,
                profile.metadata.get("source_name"),
                profile.category,
                _display_unit(profile.unit),
                profile.constant_value,
                profile.rows,
                profile.status,
            ],
            [
                formats.table_text,
                formats.table_text,
                formats.table_text,
                formats.table_text,
                formats.table_decimal_2 if profile.is_numeric else formats.table_text,
                formats.table_integer,
                formats.table_text,
            ],
        )
    last_row = 3 + len(profiles)
    sheet.freeze_panes(4, 0)
    sheet.autofilter(3, 0, last_row, len(headers) - 1)
    _set_column_widths(sheet, (34, 44, 20, 12, 18, 12, 20))
    if profiles:
        _apply_banding(sheet, 4, 0, last_row, len(headers) - 1, formats)
        _apply_status_formatting(sheet, 4, len(headers) - 1, last_row, formats)
    sheet.print_area(0, 0, last_row, len(headers) - 1)


def _write_trends(
    workbook: xlsxwriter.Workbook,
    name: str,
    category: str,
    analysis: ReportAnalysis,
    report_id: str,
    formats: ReportFormats,
    title: str,
    timezone: ZoneInfo,
) -> _TemperatureTrendLayout | None:
    sheet = workbook.add_worksheet(name)
    configure_sheet(
        sheet, COLOUR_BLUE, report_id, landscape=True, zoom=90, repeat_header_rows=(3, 3)
    )
    profiles = [
        profile
        for profile in _profiles(analysis, category)
        if profile.is_numeric
        and bool(profile.metadata.get("trend", False))
        and not profile.is_clean_constant
    ]
    headers = ["Interval", "Cycle start", "Cycle end", "Start timestamp", "End timestamp"] + [
        _column_title(profile) for profile in profiles
    ]
    sheet.set_row(0, 31)
    sheet.merge_range(0, 0, 0, len(headers) - 1, title, formats.title)
    sheet.merge_range(
        1,
        0,
        1,
        len(headers) - 1,
        "Cycle-interval averages retained for trend charting and review",
        formats.subtitle,
    )
    header_row = 3
    data_start_row = 4
    _table_header(sheet, header_row, headers, formats)
    trends = analysis.trends.get(category, ())
    data_columns = {profile.signal_id: column for column, profile in enumerate(profiles, start=5)}
    for row, trend in enumerate(trends, start=data_start_row):
        sheet.write_number(row, 0, trend.interval, formats.table_integer)
        if trend.cycle_start is not None:
            sheet.write_number(row, 1, trend.cycle_start, formats.table_integer)
        if trend.cycle_end is not None:
            sheet.write_number(row, 2, trend.cycle_end, formats.table_integer)
        if trend.start_timestamp is not None:
            sheet.write_datetime(row, 3, _local(trend.start_timestamp, timezone), formats.timestamp)
        if trend.end_timestamp is not None:
            sheet.write_datetime(row, 4, _local(trend.end_timestamp, timezone), formats.timestamp)
        for column, profile in enumerate(profiles, start=5):
            value = trend.values.get(profile.signal_id)
            if value is not None:
                sheet.write_number(row, column, value, formats.table_decimal_1)
    last_row = header_row + len(trends)
    sheet.freeze_panes(data_start_row, 5)
    sheet.autofilter(header_row, 0, last_row, len(headers) - 1)
    _set_column_widths(sheet, (10, 15, 15, 22, 22))
    for column, profile in enumerate(profiles, start=5):
        sheet.set_column(
            column, column, _header_width(_column_title(profile), minimum=16, maximum=28)
        )
    sheet.set_row(header_row, 30)
    if trends:
        _apply_banding(sheet, data_start_row, 0, last_row, len(headers) - 1, formats)
    sheet.print_area(0, 0, last_row, len(headers) - 1)
    return _TemperatureTrendLayout(
        sheet,
        tuple(profiles),
        len(trends),
        data_columns,
        header_row,
        data_start_row,
    )


_INVALID_SHEET_NAME_CHARACTERS = frozenset("[]:*?/\\")


def _sheet_name(name: str) -> str:
    """Strip characters Excel rejects in a worksheet tab name and respect the length limit."""

    return "".join(
        character for character in name if character not in _INVALID_SHEET_NAME_CHARACTERS
    )[:31]


def _write_chart_tabs(
    workbook: xlsxwriter.Workbook,
    layout: _TemperatureTrendLayout,
    report_kind: str,
    analysis: ReportAnalysis,
    report_id: str,
    timezone: ZoneInfo,
    machine_name: str,
    formats: ReportFormats,
) -> None:
    """Give every trend chart its own worksheet tab, matching the legacy report layout."""

    if not layout.profiles or not layout.row_count:
        return
    if report_kind == "temperature":
        configured = (
            ("barrel", "Barrel Temperatures"),
            ("nozzle", "Nozzle Temperatures"),
            ("tcd_01_04", "TCD01-04 Temperatures"),
            ("tcd_05_08", "TCD05-08 Temperatures"),
            ("tcd_09_12", "TCD09-12 Temperatures"),
            # Retain the legacy groups for existing customer configurations.
            ("tcd", "Water (TCD) Temperatures"),
            ("oil", "Oil Temperature"),
        )
        found = {
            str(profile.metadata.get("chart_group", "temperature")) for profile in layout.profiles
        }
        groups = [
            (group, title, title.upper(), f"{title} trend")
            for group, title in configured
            if group in found
        ]
        for group in sorted(found.difference(group for group, _ in configured)):
            title = _chart_group_title(group)
            groups.append((group, title, title.upper(), f"{title} trend"))
    else:
        ordered = (
            "injection_pressure",
            "injection_pressure_integral",
            "injection_position",
            "injection_timing",
            "screw_positions",
            "screw_timing",
            "screw_torque",
            "screw_rotations",
            # Retain the legacy group for existing customer configurations.
            "screw",
            "mould_position",
            "mould_timing",
            "clamping",
            "energy_per_cycle",
            "specific_energy",
            "cavity_pressure",
            "auxiliary_timing",
        )
        found = {str(profile.metadata.get("chart_group", "process")) for profile in layout.profiles}
        groups = []
        for group in (*ordered, *sorted(found.difference(ordered))):
            if group not in found:
                continue
            title = _chart_group_title(group)
            groups.append((group, title, title.upper(), f"{title} trend"))
    for group, tab_title, page_title, chart_title in groups:
        profiles = [
            profile
            for profile in layout.profiles
            if profile.metadata.get("chart_group") == group and _chart_profile_is_variable(profile)
        ]
        if not profiles:
            continue
        unit_groups = tuple(_split_chart_profiles_by_unit(profiles))
        for unit, compatible_profiles in unit_groups:
            values = _chart_values(analysis, compatible_profiles, report_kind)
            if not values:
                continue
            parts = list(_split_chart_profiles(compatible_profiles))
            unit_suffix = f" ({_display_unit(unit) or 'No unit'})" if len(unit_groups) > 1 else ""
            for part_number, split in enumerate(parts, start=1):
                if not split:
                    continue
                part_suffix = f" ({part_number})" if len(parts) > 1 else ""
                suffix = f"{unit_suffix}{part_suffix}"
                _write_chart_tab(
                    workbook,
                    _sheet_name(f"{tab_title}{suffix}"),
                    f"{page_title}{suffix}",
                    f"{chart_title}{suffix}",
                    report_id,
                    timezone,
                    machine_name,
                    formats,
                    split,
                    layout.data_columns,
                    layout,
                    analysis,
                    report_kind,
                )


def _split_chart_profiles(
    profiles: list[SignalProfile], maximum: int = 6
) -> Iterator[list[SignalProfile]]:
    for index in range(0, len(profiles), maximum):
        yield profiles[index : index + maximum]


def _split_chart_profiles_by_unit(
    profiles: list[SignalProfile],
) -> Iterator[tuple[str | None, list[SignalProfile]]]:
    """Keep incompatible engineering units off the same process chart."""

    groups: dict[str | None, list[SignalProfile]] = {}
    for profile in profiles:
        groups.setdefault(profile.unit, []).append(profile)
    yield from groups.items()


def _chart_group_title(group: str) -> str:
    return {
        "injection_pressure": "Injection pressure",
        "injection_pressure_integral": "Injection pressure integral",
        "injection_position": "Injection positions",
        "injection_timing": "Injection timings",
        "screw": "Screw-back process",
        "screw_positions": "Screw-back positions",
        "screw_timing": "Screw-back time",
        "screw_torque": "Dosing torque",
        "screw_rotations": "Rotations per screw-back",
        "mould_position": "Mould / ejector positions",
        "mould_timing": "Mould / ejector timings",
        "clamping": "Clamping force",
        "energy_per_cycle": "Energy per cycle",
        "specific_energy": "Specific energy",
        "cavity_pressure": "Cavity pressure",
        "auxiliary_timing": "Auxiliary timings",
    }.get(group, group.replace("_", " ").title())


def _write_chart_tab(
    workbook: xlsxwriter.Workbook,
    tab_name: str,
    page_title: str,
    chart_title: str,
    report_id: str,
    timezone: ZoneInfo,
    machine_name: str,
    formats: ReportFormats,
    profiles: list[SignalProfile],
    columns: dict[str, int],
    layout: _TemperatureTrendLayout,
    analysis: ReportAnalysis,
    report_kind: str,
) -> None:
    sheet = workbook.add_worksheet(tab_name)
    configure_sheet(sheet, COLOUR_BLUE, report_id, landscape=True, zoom=90)
    sheet.set_column(0, 20, 12)
    sheet.set_row(0, 31)
    sheet.merge_range(0, 0, 0, 20, page_title, formats.title)
    sheet.merge_range(
        1, 0, 1, 20, f"{machine_name} process report - trend overview", formats.subtitle
    )
    sheet.merge_range(
        2, 0, 2, 20, _chart_report_metadata(analysis, report_id, timezone), formats.chart_metadata
    )
    chart = workbook.add_chart({"type": "line"})
    secondary_ids = {
        profile.signal_id
        for profile in profiles
        if profile.metadata.get("chart_axis") == "secondary"
    }
    primary_profiles = [profile for profile in profiles if profile.signal_id not in secondary_ids]
    if not primary_profiles:
        primary_profiles = profiles
        secondary_ids.clear()
    for profile in profiles:
        column = columns[profile.signal_id]
        series: dict[str, object] = {
            "name": [layout.sheet.name, layout.header_row, column],
            "categories": [
                layout.sheet.name,
                layout.data_start_row,
                2,
                layout.data_start_row + layout.row_count - 1,
                2,
            ],
            "values": [
                layout.sheet.name,
                layout.data_start_row,
                column,
                layout.data_start_row + layout.row_count - 1,
                column,
            ],
            "marker": {"type": "none"},
            "line": {"color": series_colour(profile.signal_id), "width": 1.75},
        }
        if profile.signal_id in secondary_ids:
            series["y2_axis"] = True
        chart.add_series(series)
    secondary_profiles = [profile for profile in profiles if profile.signal_id in secondary_ids]
    if report_kind == "temperature":
        apply_line_chart_style(
            chart,
            chart_title,
            _temperature_axis(_chart_values(analysis, primary_profiles, report_kind)),
            _temperature_axis(_chart_values(analysis, secondary_profiles, report_kind))
            if secondary_profiles
            else None,
        )
    else:
        primary_unit = _display_unit(primary_profiles[0].unit) if primary_profiles else None
        secondary_unit = _display_unit(secondary_profiles[0].unit) if secondary_profiles else None
        apply_line_chart_style(
            chart,
            chart_title,
            _temperature_axis(_chart_values(analysis, primary_profiles, report_kind)),
            _temperature_axis(_chart_values(analysis, secondary_profiles, report_kind))
            if secondary_profiles
            else None,
            y_axis_name=primary_unit or "Value",
            secondary_y_axis_name=secondary_unit or "Value",
        )
    sheet.insert_chart("A5", chart)
    sheet.print_area(0, 0, 49, 20)


def _chart_values(
    analysis: ReportAnalysis, profiles: list[SignalProfile], report_kind: str = "temperature"
) -> list[float]:
    values: list[float] = []
    for trend in analysis.trends.get(report_kind, ()):
        for profile in profiles:
            value = trend.values.get(profile.signal_id)
            if value is None:
                continue
            values.append(value)
    return values


def _chart_report_metadata(analysis: ReportAnalysis, report_id: str, timezone: ZoneInfo) -> str:
    first = _local(analysis.first_timestamp, timezone)
    last = _local(analysis.last_timestamp, timezone)
    period = "Unavailable"
    if first is not None and last is not None:
        period = f"{first:%d-%b-%Y %H:%M} to {last:%d-%b-%Y %H:%M}"
    return f"Report ID: {report_id.split('-')[0]} | Report period: {period}"


def _chart_profile_is_variable(profile: SignalProfile) -> bool:
    return (
        profile.minimum is not None
        and profile.maximum is not None
        and profile.minimum != profile.maximum
    )


def _temperature_axis(values: list[float]) -> dict[str, float]:
    minimum = min(values)
    maximum = max(values)
    span = maximum - minimum
    # Make every plotted fluctuation use the available vertical space.  These
    # are deliberately the unrounded extrema from the report's trend values:
    # round-up/round-down padding conceals the small changes this report is
    # intended to make visible.
    if span > 0:
        return {"min": minimum, "max": maximum, "major_unit": _chart_major_unit(span)}

    # A zero-height axis cannot be rendered by Excel.  Clean constants are
    # normally excluded from charts, but this fallback also covers a varying
    # source whose interval averages happen to be identical.
    padding = max(abs(minimum) * 0.01, 0.5)
    return {"min": minimum - padding, "max": maximum + padding, "major_unit": padding}


def _chart_major_unit(span: float) -> float:
    """Return a readable grid interval without expanding the chart range."""

    target = span / 5
    magnitude = 10.0 ** math.floor(math.log10(target))
    normalized = target / magnitude
    for candidate in (1.0, 2.0, 2.5, 5.0, 10.0):
        if normalized <= candidate:
            return candidate * magnitude
    return 10.0 * magnitude


def _write_events(
    sheet: xlsxwriter.worksheet.Worksheet,
    records: RecordFactory,
    analysis: ReportAnalysis,
    rules: dict[str, Any],
    timezone: ZoneInfo,
    report_id: str,
    formats: ReportFormats,
) -> None:
    headers = [
        "Timestamp",
        "Cycle",
        "Category",
        "Event",
        "Signal",
        "Value",
        "Threshold/reference",
        "Severity",
        "Detail",
    ]
    configure_sheet(
        sheet, COLOUR_AMBER, report_id, landscape=True, zoom=80, repeat_header_rows=(3, 3)
    )
    sheet.set_row(0, 31)
    sheet.merge_range(0, 0, 0, len(headers) - 1, "EVENTS - EXCEPTIONS", formats.title)
    sheet.merge_range(
        1,
        0,
        1,
        len(headers) - 1,
        "Process and data-quality exceptions for the report period",
        formats.subtitle,
    )
    _table_header(sheet, 3, headers, formats)
    count = 0
    for count, event in enumerate(iter_events(records, analysis, rules), start=4):
        timestamp = event.timestamp.astimezone(timezone).replace(tzinfo=None)
        sheet.write_datetime(count, 0, timestamp, formats.timestamp)
        _write_row(
            sheet,
            count,
            [
                event.cycle_id,
                event.category,
                event.event_type,
                event.signal_id,
                event.value,
                event.threshold,
                event.severity,
                event.detail,
            ],
            [
                formats.table_text,
                formats.table_text,
                formats.table_text,
                formats.table_text,
                formats.table_decimal_2,
                formats.table_decimal_2,
                formats.table_text,
                formats.table_text,
            ],
            start_column=1,
        )
    last_row = count if count else 3
    sheet.freeze_panes(4, 0)
    sheet.autofilter(3, 0, last_row, len(headers) - 1)
    _set_column_widths(sheet, (22, 12, 18, 22, 28, 15, 22, 14, 60))
    sheet.set_row(3, 30)
    if count:
        _apply_banding(sheet, 4, 0, last_row, len(headers) - 1, formats)
        _apply_event_formatting(sheet, 4, last_row, formats)
    sheet.print_area(0, 0, last_row, len(headers) - 1)


def _write_raw_data(
    sheet: xlsxwriter.worksheet.Worksheet,
    records: RecordFactory,
    analysis: ReportAnalysis,
    timezone: ZoneInfo,
    report_id: str,
    formats: ReportFormats,
) -> None:
    # This tab deliberately excludes clean constants. They remain available
    # in Constant Signals without inflating every one of the raw-data rows.
    profiles = list(analysis.active_profiles)
    context_labels = {
        "run_id": "Run ID",
        "job_id": "Job ID",
        "product_id": "Product ID",
        "mould_id": "Mould ID",
        "material_batch": "Material batch",
        "active_cavities": "Active cavities",
        "sample_parts": "Sample parts",
        "rejected_parts": "Rejected parts",
    }
    headers = (
        ["Source timestamp", "Receive timestamp", "Date", "Time", "Cycle ID"]
        + [_column_title(profile) for profile in profiles]
        + [context_labels[key] for key in analysis.context_fields]
    )
    configure_sheet(
        sheet, COLOUR_MID_GREY, report_id, landscape=True, zoom=75, repeat_header_rows=(0, 0)
    )
    sheet.set_row(0, 45)
    _table_header(sheet, 0, headers, formats, raw_identity_columns=5)
    count = 0
    for count, record in enumerate(records(), start=1):
        timestamp = _local(record.source_timestamp, timezone)
        received = _local(record.received_timestamp, timezone)
        if _is_unit_metadata_record(record):
            _write_raw_unit_metadata_row(sheet, count, record, profiles, formats)
            continue
        if timestamp is not None:
            sheet.write_datetime(count, 0, timestamp, formats.raw_timestamp)
            sheet.write(count, 2, timestamp.date().isoformat(), formats.raw_text)
            sheet.write(count, 3, timestamp.time().isoformat(timespec="seconds"), formats.raw_text)
        if received is not None:
            sheet.write_datetime(count, 1, received, formats.raw_timestamp)
        sheet.write(count, 4, record.cycle_id, formats.raw_text)
        values = {
            signal.signal_id: signal.value if signal.value is not None else signal.raw_value
            for signal in record.signals
        }
        for column, profile in enumerate(profiles, start=5):
            value = values.get(profile.signal_id)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                number_format = (
                    formats.raw_decimal_1
                    if profile.category == "temperature"
                    else formats.raw_decimal_2
                )
                sheet.write_number(count, column, value, number_format)
            elif value is not None:
                sheet.write(count, column, value, formats.raw_text)
        for column, key in enumerate(analysis.context_fields, start=5 + len(profiles)):
            value = record.run_id if key == "run_id" else record.production_context.get(key)
            if value is not None:
                sheet.write(count, column, value, formats.raw_text)
    sheet.freeze_panes(1, 5)
    sheet.autofilter(0, 0, count, len(headers) - 1)
    _set_column_widths(sheet, (22, 22, 14, 12, 14))
    for column, profile in enumerate(profiles, start=5):
        sheet.set_column(
            column, column, _header_width(_column_title(profile), minimum=14, maximum=28)
        )
    sheet.print_area(0, 0, count, len(headers) - 1)
    if analysis.context_fields:
        sheet.set_column(5 + len(profiles), len(headers) - 1, 22)


def _is_unit_metadata_record(record: ProcessRecord) -> bool:
    """Recognise an exported engineering-units row without modifying its retained data."""

    values = [signal.value for signal in record.signals if signal.value is not None]
    return (
        record.source_timestamp is None
        and record.cycle_id is None
        and bool(values)
        and all(isinstance(value, str) for value in values)
    )


def _write_raw_unit_metadata_row(
    sheet: xlsxwriter.worksheet.Worksheet,
    row: int,
    record: ProcessRecord,
    profiles: list[SignalProfile],
    formats: ReportFormats,
) -> None:
    """Render a retained source units row as small, muted engineering metadata."""

    sheet.set_row(row, 14, formats.raw_units_text)
    for column in (0, 1, 2, 3, 4):
        sheet.write_blank(row, column, None, formats.raw_units_text)
    values = {signal.signal_id: signal.value for signal in record.signals}
    for column, profile in enumerate(profiles, start=5):
        value = values.get(profile.signal_id)
        if value is None:
            sheet.write_blank(row, column, None, formats.raw_units_text)
        else:
            sheet.write(row, column, value, formats.raw_units_text)


def _write_manifest(
    sheet: xlsxwriter.worksheet.Worksheet,
    analysis: ReportAnalysis,
    report_id: str,
    machine_name: str,
    source_manifest: dict[str, object],
    formats: ReportFormats,
) -> None:
    configure_sheet(sheet, COLOUR_MID_GREY, report_id, landscape=False, zoom=90)
    sheet.set_column("A:A", 30)
    sheet.set_column("B:B", 78)
    sheet.set_row(0, 31)
    sheet.merge_range("A1:B1", "SOURCE MANIFEST / AUDIT TRAIL", formats.title)
    sheet.merge_range(
        "A2:B2",
        "Traceability record for the automatically generated DEMO-01 process report",
        formats.subtitle,
    )
    groups = (
        (
            "Report Identification",
            (
                ("Machine", machine_name),
                ("Report ID", report_id),
                (
                    "Report period start",
                    analysis.first_timestamp.isoformat() if analysis.first_timestamp else None,
                ),
                (
                    "Report period end",
                    analysis.last_timestamp.isoformat() if analysis.last_timestamp else None,
                ),
                ("Acquisition hours", source_manifest.get("acquisition_hours")),
                ("Generated timestamp", datetime.now(UTC).isoformat()),
            ),
        ),
        (
            "Source Data",
            (
                ("Original filename", source_manifest.get("original_filenames")),
                ("Original file size", source_manifest.get("original_sizes")),
                ("Original source SHA-256", source_manifest.get("source_checksums")),
                ("Source encoding", source_manifest.get("source_encoding")),
                ("Source delimiter", source_manifest.get("source_delimiter")),
                (
                    "Physical source column count",
                    source_manifest.get("physical_source_column_count"),
                ),
                ("Named source-field count", source_manifest.get("named_source_field_count")),
                (
                    "Ignored blank trailing column count",
                    source_manifest.get("ignored_blank_trailing_column_count"),
                ),
                ("Source ordering", source_manifest.get("source_ordering")),
                ("Exact source headers", source_manifest.get("source_headers")),
                ("Raw archive identifiers", source_manifest.get("raw_archive_ids")),
                ("Raw archive SHA-256", source_manifest.get("raw_archive_checksums")),
                ("Row count", analysis.record_count),
                ("Signal/column count", len(analysis.profiles)),
                (
                    "First timestamp",
                    analysis.first_timestamp.isoformat() if analysis.first_timestamp else None,
                ),
                (
                    "Last timestamp",
                    analysis.last_timestamp.isoformat() if analysis.last_timestamp else None,
                ),
                ("First cycle", analysis.first_cycle),
                ("Last cycle", analysis.last_cycle),
            ),
        ),
        (
            "Software / System",
            (
                ("Raspberry Pi hostname", source_manifest.get("hostname")),
                ("Application version", source_manifest.get("application_version", "0.1.0")),
                ("Configuration version", source_manifest.get("configuration_version")),
                ("Acquisition provider", source_manifest.get("acquisition_provider")),
            ),
        ),
        (
            "Validation",
            (
                (
                    "Workbook SHA-256",
                    "Maintained in external SQLite audit state after atomic promotion",
                ),
            ),
        ),
    )
    row = 3
    for heading, values in groups:
        sheet.merge_range(row, 0, row, 1, heading, formats.section_header)
        row += 1
        for label, value in values:
            sheet.write(row, 0, label, formats.manifest_label)
            sheet.write(row, 1, value, formats.manifest_value)
            row += 1
        row += 1
    sheet.print_area(0, 0, row - 2, 1)


def _profiles(analysis: ReportAnalysis, category: str) -> tuple[SignalProfile, ...]:
    return tuple(profile for profile in analysis.profiles.values() if profile.category == category)


def _profile_average(analysis: ReportAnalysis, signal_id: str) -> float | None:
    profile = analysis.profiles.get(signal_id)
    return profile.average if profile is not None else None


def _energy_error_count(analysis: ReportAnalysis) -> int:
    return sum(
        profile.parse_failures
        for signal_id, profile in analysis.profiles.items()
        if signal_id.startswith("energy_")
    )


def _write_summary_block(
    sheet: xlsxwriter.worksheet.Worksheet,
    start_row: int,
    start_column: int,
    title: str,
    values: Sequence[tuple[str, object, xlsxwriter.format.Format | None]],
    formats: ReportFormats,
) -> None:
    sheet.merge_range(
        start_row, start_column, start_row, start_column + 1, title, formats.section_header
    )
    for row, (label, value, value_format) in enumerate(values, start=start_row + 1):
        sheet.write(row, start_column, label, formats.label)
        _write_value(sheet, row, start_column + 1, value, value_format or formats.value_bold)


def _write_parallel_summary_blocks(
    sheet: xlsxwriter.worksheet.Worksheet,
    start_row: int,
    left_title: str,
    left_values: Sequence[tuple[str, object, xlsxwriter.format.Format | None]],
    right_title: str,
    right_values: Sequence[tuple[str, object, xlsxwriter.format.Format | None]],
    formats: ReportFormats,
) -> None:
    """Write side-by-side Summary sections without backtracking in constant-memory mode."""

    sheet.merge_range(start_row, 0, start_row, 1, left_title, formats.section_header)
    sheet.merge_range(start_row, 3, start_row, 4, right_title, formats.section_header)
    for offset in range(max(len(left_values), len(right_values))):
        row = start_row + 1 + offset
        if offset < len(left_values):
            label, value, value_format = left_values[offset]
            sheet.write(row, 0, label, formats.label)
            _write_value(sheet, row, 1, value, value_format or formats.value_bold)
        if offset < len(right_values):
            label, value, value_format = right_values[offset]
            sheet.write(row, 3, label, formats.label)
            _write_value(sheet, row, 4, value, value_format or formats.value_bold)


def _table_header(
    sheet: xlsxwriter.worksheet.Worksheet,
    row: int,
    headers: list[str],
    formats: ReportFormats,
    raw_identity_columns: int = 0,
) -> None:
    for column, header in enumerate(headers):
        cell_format = (
            formats.raw_identity_header if column < raw_identity_columns else formats.table_header
        )
        sheet.write(row, column, header, cell_format)


def _write_row(
    sheet: xlsxwriter.worksheet.Worksheet,
    row: int,
    values: list[object],
    value_formats: list[xlsxwriter.format.Format],
    start_column: int = 0,
) -> None:
    for offset, (value, value_format) in enumerate(zip(values, value_formats, strict=True)):
        _write_value(sheet, row, start_column + offset, value, value_format)


def _set_column_widths(sheet: xlsxwriter.worksheet.Worksheet, widths: Sequence[float]) -> None:
    for column, width in enumerate(widths):
        sheet.set_column(column, column, width)


def _header_width(header: str, minimum: float, maximum: float) -> float:
    """Size from configured header text only; never scan the raw-data dataset."""

    return min(maximum, max(minimum, len(header) + 2))


def _apply_banding(
    sheet: xlsxwriter.worksheet.Worksheet,
    first_row: int,
    first_column: int,
    last_row: int,
    last_column: int,
    formats: ReportFormats,
) -> None:
    sheet.conditional_format(
        first_row,
        first_column,
        last_row,
        last_column,
        {"type": "formula", "criteria": "=MOD(ROW(),2)=0", "format": formats.band},
    )


def _apply_nonzero_formatting(
    sheet: xlsxwriter.worksheet.Worksheet,
    first_row: int,
    columns: tuple[int, ...],
    last_row: int,
    formats: ReportFormats,
) -> None:
    for column in columns:
        sheet.conditional_format(
            first_row,
            column,
            last_row,
            column,
            {"type": "cell", "criteria": ">", "value": 0, "format": formats.status_warning},
        )


def _apply_status_formatting(
    sheet: xlsxwriter.worksheet.Worksheet,
    first_row: int,
    column: int,
    last_row: int,
    formats: ReportFormats,
) -> None:
    sheet.conditional_format(
        first_row,
        column,
        last_row,
        column,
        {
            "type": "text",
            "criteria": "containing",
            "value": "Data quality",
            "format": formats.status_warning,
        },
    )
    sheet.conditional_format(
        first_row,
        column,
        last_row,
        column,
        {
            "type": "text",
            "criteria": "containing",
            "value": "Active",
            "format": formats.status_normal,
        },
    )
    sheet.conditional_format(
        first_row,
        column,
        last_row,
        column,
        {
            "type": "text",
            "criteria": "containing",
            "value": "Constant",
            "format": formats.status_normal,
        },
    )


def _apply_event_formatting(
    sheet: xlsxwriter.worksheet.Worksheet,
    first_row: int,
    last_row: int,
    formats: ReportFormats,
) -> None:
    sheet.conditional_format(
        first_row,
        2,
        last_row,
        2,
        {
            "type": "text",
            "criteria": "containing",
            "value": "data",
            "format": formats.status_warning,
        },
    )
    sheet.conditional_format(
        first_row,
        7,
        last_row,
        7,
        {
            "type": "text",
            "criteria": "containing",
            "value": "error",
            "format": formats.status_error,
        },
    )
    sheet.conditional_format(
        first_row,
        7,
        last_row,
        7,
        {
            "type": "text",
            "criteria": "containing",
            "value": "warning",
            "format": formats.status_warning,
        },
    )


def _write_value(
    sheet: xlsxwriter.worksheet.Worksheet,
    row: int,
    column: int,
    value: object,
    value_format: xlsxwriter.format.Format | None,
) -> None:
    if isinstance(value, datetime):
        sheet.write_datetime(row, column, value, value_format)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        sheet.write_number(row, column, value, value_format)
    else:
        sheet.write(row, column, value, value_format)


def _local(value: datetime | None, timezone: ZoneInfo) -> datetime | None:
    return value.astimezone(timezone).replace(tzinfo=None) if value is not None else None


def _wall_seconds(first: datetime | None, last: datetime | None) -> float | None:
    return (last - first).total_seconds() if first is not None and last is not None else None


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _span(profile: SignalProfile) -> float | None:
    return (
        profile.maximum - profile.minimum
        if profile.maximum is not None and profile.minimum is not None
        else None
    )


def _column_title(profile: SignalProfile) -> str:
    unit = _display_unit(profile.unit)
    return f"{profile.display_name} ({unit})" if unit else profile.display_name


def _display_unit(unit: str | None) -> str | None:
    """Use standard engineering notation without changing underlying measurements."""

    return "°C" if unit == "C" else unit


def _filename(machine_name: str, report_id: str) -> str:
    safe_machine = "".join(
        character for character in machine_name if character.isalnum() or character in "-_"
    )
    return (
        f"{safe_machine}_process_report_{datetime.now(UTC):%Y%m%d}_{report_id.split('-')[0]}.xlsx"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("r+b") as file:
        os.fsync(file.fileno())


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _phase(metrics: PipelineMetrics | None, name: str) -> AbstractContextManager[None]:
    return metrics.phase(name) if metrics is not None else nullcontext()
