"""Chart styling and stable series colours for DEMO-01 reports."""

from __future__ import annotations

import hashlib
from typing import Any

from .styles import (
    COLOUR_AMBER,
    COLOUR_BLUE,
    COLOUR_BORDER,
    COLOUR_DARK_TEXT,
    COLOUR_GREEN,
    COLOUR_MID_GREY,
    COLOUR_NAVY,
    COLOUR_RED,
    COLOUR_TEAL,
    COLOUR_WHITE,
    FONT_NAME,
)

_KNOWN_SERIES_COLOURS = {
    "feed_zone_temperature": COLOUR_NAVY,
    "zone_1_temperature": COLOUR_BLUE,
    "zone_2_temperature": COLOUR_TEAL,
    "zone_3_temperature": COLOUR_GREEN,
    "zone_4_temperature": COLOUR_AMBER,
    "nozzle_1_temperature": COLOUR_RED,
    "oil_temperature": "#546E7A",
}
_FALLBACK_SERIES_COLOURS = (
    COLOUR_BLUE,
    COLOUR_TEAL,
    COLOUR_GREEN,
    COLOUR_AMBER,
    COLOUR_RED,
    "#8064A2",
)
_TCD_SERIES_COLOURS = (
    "#2F75B5",
    "#31859C",
    "#548235",
    "#BF9000",
    "#C00000",
    "#8064A2",
    "#4F81BD",
    "#76933C",
    "#C0504D",
    "#7F7F7F",
    "#17365D",
    "#A64D79",
)


def series_colour(signal_id: str) -> str:
    """Return a stable colour even for a newly configured signal."""

    if signal_id in _KNOWN_SERIES_COLOURS:
        return _KNOWN_SERIES_COLOURS[signal_id]
    tcd_index = _tcd_index(signal_id)
    if tcd_index is not None and tcd_index < len(_TCD_SERIES_COLOURS):
        return _TCD_SERIES_COLOURS[tcd_index]
    digest = hashlib.sha256(signal_id.encode("utf-8")).digest()
    return _FALLBACK_SERIES_COLOURS[digest[0] % len(_FALLBACK_SERIES_COLOURS)]


def _tcd_index(signal_id: str) -> int | None:
    """Recognise the configured TCD01-TCD12 signal IDs without affecting other signals."""

    prefix = "tcd_"
    suffix = "_temperature"
    if not signal_id.startswith(prefix) or not signal_id.endswith(suffix):
        return None
    number = signal_id[len(prefix) : -len(suffix)]
    if not number.isdigit():
        return None
    index = int(number) - 1
    return index if index >= 0 else None


def apply_line_chart_style(
    chart: Any,
    title: str,
    y_axis: dict[str, Any],
    secondary_y_axis: dict[str, Any] | None = None,
    y_axis_name: str = "Temperature (°C)",
    secondary_y_axis_name: str = "Feed temperature (°C)",
    x_axis_name: str = "Cycle number",
) -> None:
    """Apply a restrained industrial style without affecting plotted data."""

    chart.set_chartarea({"fill": {"color": COLOUR_WHITE}, "border": {"none": True}})
    chart.set_plotarea({"fill": {"color": COLOUR_WHITE}, "border": {"none": True}})
    chart.set_title(
        {
            "name": title,
            "name_font": {"name": FONT_NAME, "size": 14, "bold": True, "color": COLOUR_DARK_TEXT},
        }
    )
    chart.set_x_axis(
        {
            "name": x_axis_name,
            "num_format": "#,##0",
            "interval_unit": 3,
            "name_font": {"name": FONT_NAME, "size": 10, "bold": True, "color": COLOUR_DARK_TEXT},
            "num_font": {"name": FONT_NAME, "size": 9, "color": COLOUR_DARK_TEXT},
            "line": {"color": COLOUR_MID_GREY},
            "major_gridlines": {"visible": False},
        }
    )
    chart.set_y_axis(_axis_options(y_axis_name, y_axis))
    if secondary_y_axis is not None:
        chart.set_y2_axis(_axis_options(secondary_y_axis_name, secondary_y_axis))
    chart.set_legend(
        {
            "position": "bottom",
            "font": {"name": FONT_NAME, "size": 10, "color": COLOUR_DARK_TEXT},
        }
    )
    chart.set_size({"width": 1450, "height": 780})


def _axis_options(name: str, range_options: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        **range_options,
        "name_font": {"name": FONT_NAME, "size": 10, "bold": True, "color": COLOUR_DARK_TEXT},
        "num_font": {"name": FONT_NAME, "size": 9, "color": COLOUR_DARK_TEXT},
        "line": {"color": COLOUR_MID_GREY},
        "major_gridlines": {"visible": True, "line": {"color": COLOUR_BORDER}},
    }
