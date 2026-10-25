"""Replay a local DEMO-01 CSV fixture through the complete retained report path."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import replace
from pathlib import Path

from edge_logger.config import load_config
from edge_logger.runtime import LoggerRuntime
from edge_logger.storage.audit_db import AuditDatabase


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--config", type=Path, default=Path("config/config.example.yaml"))
    parser.add_argument("--output", type=Path, default=Path("build/demo-reference-replay"))
    args = parser.parse_args()
    config = load_config(args.config)
    output = args.output.resolve()
    config = replace(
        config,
        raw_path=output / "raw",
        reports_path=output / "reports",
        state_path=output / "state",
    )
    database = AuditDatabase(config.state_path / "state.sqlite3")
    started = time.perf_counter()
    try:
        result = asyncio.run(
            LoggerRuntime(config, database).import_file(args.source, force_report=True)
        )
        period = database.report_period(result.sealed_report_ids[0])
        report = Path(str(period["workbook_path"]))
        reports = []
        for report_id in result.sealed_report_ids:
            row = database.report_period(report_id)
            export = json.loads(database.application_state(f"process_export:{row['id']}") or "{}")
            reports.append(
                {
                    "report_id": report_id,
                    "record_count": row["record_count"],
                    "state": row["state"],
                    "workbook": row["workbook_path"],
                    "workbook_sha256": row["workbook_sha256"],
                    "process_export": export,
                    "combined_attachment_bytes": Path(row["workbook_path"]).stat().st_size
                    + Path(export["path"]).stat().st_size,
                }
            )
        canonical_count = database.connection.execute(
            "SELECT COUNT(*) FROM process_records"
        ).fetchone()[0]
        print(
            json.dumps(
                {
                    "source": str(args.source),
                    "inspection": {
                        "rows": result.inspection.row_count,
                        "physical_columns": result.inspection.physical_column_count,
                        "named_columns": result.inspection.named_column_count,
                        "trailing_blank_columns": result.inspection.ignored_blank_trailing_columns,
                        "source_ordering": result.inspection.source_ordering,
                    },
                    "elapsed_seconds": round(time.perf_counter() - started, 3),
                    "metrics": result.metrics,
                    "workbook": str(report),
                    "workbook_size_bytes": report.stat().st_size,
                    "workbook_sha256": period["workbook_sha256"],
                    "workbook_state": period["state"],
                    "canonical_records": canonical_count,
                    "reports": reports,
                },
                indent=2,
            )
        )
    finally:
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
