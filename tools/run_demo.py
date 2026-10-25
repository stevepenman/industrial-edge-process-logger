"""Generate fictional process records and verify the complete offline pipeline."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from edge_logger.config import load_config
from edge_logger.runtime import LoggerRuntime
from edge_logger.storage.audit_db import AuditDatabase
from edge_logger.storage.receiver import ingest_export

PROJECT = Path(__file__).resolve().parents[1]


def synthetic_rows(count: int):
    """Deterministic formulas only; no measurements are sampled from another file."""
    start = datetime(2026, 1, 1, 12, tzinfo=UTC)
    for index in range(count):
        timestamp = start + timedelta(seconds=index * 15)
        pressure = (
            "OVERRANGE" if index and index % 47 == 0 else f"{100 + 4 * math.sin(index / 9):.3f}"
        )
        cycle_time = 15 + 0.2 * math.sin(index / 7) + (3 if index and index % 37 == 0 else 0)
        yield [
            timestamp.strftime("%d.%m.%Y"),
            timestamp.strftime("%H:%M:%S"),
            str(1000 + index),
            pressure,
            f"{cycle_time:.3f}",
            f"{200 + 1.5 * math.cos(index / 11):.3f}",
            str(int(bool(index and index % 29 == 0))),
            f"{12 + 0.4 * math.cos(index / 6):.3f}",
        ]


def run(output: Path, rows: int) -> dict[str, object]:
    if not 1 <= rows <= 100_000:
        raise ValueError("--rows must be between 1 and 100000")
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    marker = output / "demo-settings.json"
    source = output / "synthetic_cycles.tsv"
    if marker.exists() and json.loads(marker.read_text())["rows"] != rows:
        raise ValueError("Choose a new --output directory when changing the dataset size")
    if source.exists() and not marker.exists():
        raise ValueError("Refusing to overwrite an unrecognised existing dataset")
    marker.write_text(json.dumps({"synthetic": True, "rows": rows}), encoding="utf-8")
    with source.open("w", encoding="utf-16", newline="") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "Date",
                "Time",
                "Cycles",
                "Pressure",
                "Cycle time",
                "Temperature",
                "Rejected parts",
                "Energy",
            ]
        )
        writer.writerow(["", "", "", "bar", "s", "C", "parts", "Wh"])
        writer.writerows(synthetic_rows(rows))
    base = load_config(PROJECT / "config" / "config.example.yaml")
    production = {
        "job_id": "DEMO-JOB-001",
        "product_id": "DEMO-PART-A",
        "mould_id": "DEMO-MOULD-01",
        "material_batch": "SYNTHETIC-BATCH-A",
        "active_cavities": 2,
        "sample_parts": 0,
    }
    config = replace(
        base,
        raw_path=output / "raw",
        reports_path=output / "reports",
        state_path=output / "state",
        rules={**base.rules, "long_cycle_seconds": 16},
        raw={**base.raw, "production": production},
    )
    database = AuditDatabase(config.state_path / "state.sqlite3")
    try:
        runtime = LoggerRuntime(config, database)
        with patch("edge_logger.runtime.socket.gethostname", return_value="demo-edge-node"):
            result = asyncio.run(runtime.import_file(source, force_report=True))
        periods = [row for row in database.periods() if row["workbook_path"]]
        receipts = []
        for period in periods:
            metadata = json.loads(
                database.application_state(f"process_export:{period['id']}") or "{}"
            )
            receipt = ingest_export(Path(metadata["path"]), output / "receiver" / "cycles.sqlite3")
            if not ingest_export(Path(metadata["path"]), output / "receiver" / "cycles.sqlite3")[
                "duplicate"
            ]:
                raise RuntimeError("Receiver duplicate protection failed")
            receipts.append(receipt)
        canonical = database.connection.execute("SELECT COUNT(*) FROM process_records").fetchone()[
            0
        ]
        with sqlite3.connect(output / "receiver" / "cycles.sqlite3") as receiver:
            received = receiver.execute("SELECT COUNT(*) FROM cycles").fetchone()[0]
        if (
            canonical != rows
            or received != rows
            or sum(row["record_count"] for row in periods) != rows
        ):
            raise RuntimeError("End-to-end record counts do not reconcile")
        summary = {
            "synthetic_data_only": True,
            "canonical_records": canonical,
            "receiver_records": received,
            "reports": len(periods),
            "repeat_import": result.duplicate,
            "network_used": False,
            "output": output.relative_to(PROJECT).as_posix()
            if output.is_relative_to(PROJECT)
            else output.name,
            "workbooks": [Path(row["workbook_path"]).name for row in periods],
            "storage_receipts": [item["receipt"] for item in receipts],
        }
        (output / "demo-result.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary
    finally:
        database.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROJECT / "build" / "demo")
    parser.add_argument("--rows", type=int, default=120)
    args = parser.parse_args()
    print(json.dumps(run(args.output, args.rows), indent=2))


if __name__ == "__main__":
    main()
