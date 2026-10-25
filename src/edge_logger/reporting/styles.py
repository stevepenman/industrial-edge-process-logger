"""Reusable visual system for DEMO-01 Excel process reports."""

from __future__ import annotations

from dataclasses import dataclass

import xlsxwriter  # type: ignore[import-untyped]

COLOUR_NAVY = "#17365D"
COLOUR_BLUE = "#2F75B5"
COLOUR_TEAL = "#31859C"
COLOUR_LIGHT_BLUE = "#D9EAF7"
COLOUR_VERY_LIGHT_BLUE = "#EAF2F8"
COLOUR_DARK_TEXT = "#263238"
COLOUR_MID_GREY = "#7F8C8D"
COLOUR_LIGHT_GREY = "#F3F5F7"
COLOUR_BORDER = "#D9E1E8"
COLOUR_WHITE = "#FFFFFF"
COLOUR_GREEN = "#548235"
COLOUR_LIGHT_GREEN = "#E2F0D9"
COLOUR_AMBER = "#BF9000"
COLOUR_LIGHT_AMBER = "#FFF2CC"
COLOUR_RED = "#C00000"
COLOUR_LIGHT_RED = "#FCE4D6"

FONT_NAME = "Calibri"


@dataclass(frozen=True, slots=True)
class ReportFormats:
    """Shared XlsxWriter formats. One workbook creates this set once."""

    title: xlsxwriter.format.Format
    subtitle: xlsxwriter.format.Format
    section_header: xlsxwriter.format.Format
    table_header: xlsxwriter.format.Format
    raw_identity_header: xlsxwriter.format.Format
    label: xlsxwriter.format.Format
    value: xlsxwriter.format.Format
    value_bold: xlsxwriter.format.Format
    integer: xlsxwriter.format.Format
    decimal_1: xlsxwriter.format.Format
    decimal_2: xlsxwriter.format.Format
    decimal_3: xlsxwriter.format.Format
    percentage: xlsxwriter.format.Format
    timestamp: xlsxwriter.format.Format
    table_text: xlsxwriter.format.Format
    table_integer: xlsxwriter.format.Format
    table_decimal_1: xlsxwriter.format.Format
    table_decimal_2: xlsxwriter.format.Format
    table_decimal_3: xlsxwriter.format.Format
    raw_text: xlsxwriter.format.Format
    raw_decimal_1: xlsxwriter.format.Format
    raw_decimal_2: xlsxwriter.format.Format
    raw_timestamp: xlsxwriter.format.Format
    raw_units_text: xlsxwriter.format.Format
    chart_metadata: xlsxwriter.format.Format
    band: xlsxwriter.format.Format
    manifest_label: xlsxwriter.format.Format
    manifest_value: xlsxwriter.format.Format
    status_normal: xlsxwriter.format.Format
    status_warning: xlsxwriter.format.Format
    status_error: xlsxwriter.format.Format


def create_report_formats(workbook: xlsxwriter.Workbook) -> ReportFormats:
    """Create the finite format palette used across a generated workbook."""

    base = {"font_name": FONT_NAME, "font_color": COLOUR_DARK_TEXT, "font_size": 10}
    bordered = {**base, "border": 1, "border_color": COLOUR_BORDER}
    table = {**bordered, "valign": "vcenter"}
    return ReportFormats(
        title=workbook.add_format(
            {
                "font_name": FONT_NAME,
                "font_color": COLOUR_WHITE,
                "font_size": 20,
                "bold": True,
                "bg_color": COLOUR_NAVY,
                "align": "left",
                "valign": "vcenter",
            }
        ),
        subtitle=workbook.add_format(
            {**base, "font_size": 10, "font_color": COLOUR_MID_GREY, "italic": True}
        ),
        section_header=workbook.add_format(
            {
                "font_name": FONT_NAME,
                "font_color": COLOUR_WHITE,
                "font_size": 11,
                "bold": True,
                "bg_color": COLOUR_BLUE,
                "align": "left",
                "valign": "vcenter",
            }
        ),
        table_header=workbook.add_format(
            {
                "font_name": FONT_NAME,
                "font_color": COLOUR_WHITE,
                "font_size": 10,
                "bold": True,
                "bg_color": COLOUR_NAVY,
                "border": 1,
                "border_color": COLOUR_BORDER,
                "align": "center",
                "valign": "vcenter",
                "text_wrap": True,
            }
        ),
        raw_identity_header=workbook.add_format(
            {
                "font_name": FONT_NAME,
                "font_color": COLOUR_DARK_TEXT,
                "font_size": 10,
                "bold": True,
                "bg_color": COLOUR_LIGHT_BLUE,
                "border": 1,
                "border_color": COLOUR_BORDER,
                "align": "center",
                "valign": "vcenter",
                "text_wrap": True,
            }
        ),
        label=workbook.add_format(
            {**bordered, "bold": True, "bg_color": COLOUR_VERY_LIGHT_BLUE, "align": "left"}
        ),
        value=workbook.add_format({**bordered, "align": "left"}),
        value_bold=workbook.add_format(
            {**bordered, "bold": True, "font_size": 11, "align": "right"}
        ),
        integer=workbook.add_format({**bordered, "num_format": "#,##0", "align": "right"}),
        decimal_1=workbook.add_format({**bordered, "num_format": "0.0", "align": "right"}),
        decimal_2=workbook.add_format({**bordered, "num_format": "0.00", "align": "right"}),
        decimal_3=workbook.add_format({**bordered, "num_format": "0.000", "align": "right"}),
        percentage=workbook.add_format({**bordered, "num_format": "0.00%", "align": "right"}),
        timestamp=workbook.add_format(
            {**bordered, "num_format": "dd-mmm-yyyy hh:mm:ss", "align": "left"}
        ),
        table_text=workbook.add_format({**table, "align": "left"}),
        table_integer=workbook.add_format({**table, "num_format": "#,##0", "align": "right"}),
        table_decimal_1=workbook.add_format({**table, "num_format": "0.0", "align": "right"}),
        table_decimal_2=workbook.add_format({**table, "num_format": "0.00", "align": "right"}),
        table_decimal_3=workbook.add_format({**table, "num_format": "0.000", "align": "right"}),
        raw_text=workbook.add_format({**base, "align": "left"}),
        raw_decimal_1=workbook.add_format({**base, "num_format": "0.0", "align": "right"}),
        raw_decimal_2=workbook.add_format({**base, "num_format": "0.00", "align": "right"}),
        raw_timestamp=workbook.add_format(
            {**base, "num_format": "dd-mmm-yyyy hh:mm:ss", "align": "left"}
        ),
        raw_units_text=workbook.add_format(
            {
                **base,
                "font_size": 9,
                "font_color": COLOUR_MID_GREY,
                "italic": True,
                "bg_color": COLOUR_VERY_LIGHT_BLUE,
                "align": "left",
            }
        ),
        chart_metadata=workbook.add_format(
            {**base, "font_size": 9, "font_color": COLOUR_MID_GREY, "align": "left"}
        ),
        band=workbook.add_format({"bg_color": COLOUR_VERY_LIGHT_BLUE}),
        manifest_label=workbook.add_format(
            {**bordered, "bold": True, "bg_color": COLOUR_LIGHT_GREY, "valign": "top"}
        ),
        manifest_value=workbook.add_format(
            {**bordered, "font_size": 9, "text_wrap": True, "valign": "top"}
        ),
        status_normal=workbook.add_format(
            {"bg_color": COLOUR_LIGHT_GREEN, "font_color": COLOUR_GREEN}
        ),
        status_warning=workbook.add_format(
            {"bg_color": COLOUR_LIGHT_AMBER, "font_color": COLOUR_AMBER}
        ),
        status_error=workbook.add_format({"bg_color": COLOUR_LIGHT_RED, "font_color": COLOUR_RED}),
    )


def configure_sheet(
    sheet: xlsxwriter.worksheet.Worksheet,
    tab_colour: str,
    report_id: str,
    *,
    landscape: bool,
    zoom: int,
    repeat_header_rows: tuple[int, int] | None = None,
) -> None:
    """Apply common worksheet navigation and print settings."""

    sheet.hide_gridlines(2)
    sheet.set_tab_color(tab_colour)
    if landscape:
        sheet.set_landscape()
    else:
        sheet.set_portrait()
    sheet.set_paper(9)
    sheet.fit_to_pages(1, 0)
    sheet.center_horizontally()
    sheet.set_margins(left=0.3, right=0.3, top=0.5, bottom=0.5)
    sheet.set_footer(
        f"&LDEMO-01 Process Report&CReport ID: {report_id.split('-')[0]}&RPage &P of &N"
    )
    sheet.set_zoom(zoom)
    if repeat_header_rows is not None:
        sheet.repeat_rows(*repeat_header_rows)
