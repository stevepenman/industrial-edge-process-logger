"""Synthetic fixtures only; no operational datasets are required."""

from pathlib import Path

from edge_logger.acquisition.file_export import FileExportImporter
from edge_logger.analysis.pipeline import analyse_records, iter_events
from edge_logger.models import QualityStatus


def test_demo_export_handles_units_trailing_column_reverse_order_and_energy_overflow(
    tmp_path: Path,
) -> None:
    source = tmp_path / "synthetic_export.tsv"
    source.write_text(
        "Date\tTime\tCycles\t Feed zone actual temp.\tEnergy/cycle total\tBad parts\t\n"
        "\t\t\t°C\tWh\t\n"
        + "".join(
            f"03.07.26\t12:{minute:02d}:00\t{cycle}\t{(f'{200 + cycle / 10:.1f}').replace('.', ',')}"
            + f"\t{'OVERRANGE' if cycle == 10 else '12,3'}\t{1 if cycle == 1 else 0}\t\n"
            for cycle, minute in zip(range(10, 0, -1), range(45, -5, -5), strict=True)
        ),
        encoding="utf-16",
    )
    registry: dict[str, dict[str, object]] = {
        "feed_zone_temperature": {
            "source_name": "Feed zone actual temp.",
            "display_name": "Feed Zone",
            "category": "temperature",
            "unit": "C",
            "expected_datatype": "float",
            "trend": True,
            "chart_group": "barrel",
        },
        "energy_cycle_total_wh": {
            "source_name": "Energy/cycle total",
            "display_name": "Total energy per cycle",
            "category": "process",
            "unit": "Wh",
            "expected_datatype": "float",
            "trend": True,
            "chart_group": "energy_per_cycle",
        },
        "bad_parts": {
            "source_name": "Bad parts",
            "category": "quality",
            "expected_datatype": "float",
            "exception_rule": "bad_part",
        },
    }
    importer = FileExportImporter("DEMO-01", registry)

    inspection = importer.inspect(source)
    records = list(importer.records(source))

    assert inspection.row_count == 10
    assert inspection.physical_column_count == 7
    assert inspection.named_column_count == 6
    assert inspection.ignored_blank_trailing_columns == 1
    assert inspection.units[3:6] == ("°C", "Wh", "")
    assert inspection.source_ordering == "descending"
    assert records[0].cycle_id == "10"
    feed, energy, bad_parts = records[0].signals
    assert feed.signal_id == "feed_zone_temperature"
    assert feed.source_name == " Feed zone actual temp."
    assert energy.quality is QualityStatus.INVALID_NUMERIC
    assert energy.value is None and energy.raw_value == "OVERRANGE"
    assert bad_parts.value == 0.0

    analysis = analyse_records(
        lambda: iter(records), registry, "Europe/London", 5, {"long_cycle_seconds": None}
    )
    energy_profile = analysis.profiles["energy_cycle_total_wh"]
    assert energy_profile.parse_failures == 1
    assert not energy_profile.is_clean_constant
    assert analysis.long_cycle_count == 0
    events = list(iter_events(lambda: iter(records), analysis, {"long_cycle_seconds": None}))
    assert any(
        event.event_type == "Source numeric overflow / invalid numeric value" for event in events
    )
    assert any(event.event_type == "Source data gap" for event in events)
