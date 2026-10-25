"""Reconcile archive mirrors against transactionally committed process records."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Iterator
from pathlib import Path

from ..models import ProcessRecord, ReportPeriodState
from .audit_db import AuditDatabase
from .raw_archive import archive_checksum, read_records, rebuild_archive, record_from_payload


def recover_archives(database: AuditDatabase, raw_directory: Path) -> None:
    directory = raw_directory / "records"
    directory.mkdir(parents=True, exist_ok=True)
    registered = {
        Path(row["path"]).resolve()
        for row in database.connection.execute("SELECT path FROM raw_segments")
    }
    # Adopt legacy unregistered segments. New segments are registered before the first commit.
    for path in directory.glob("*.jsonl"):
        name = path.name.lstrip(".").replace(".partial.jsonl", ".jsonl")
        final = directory / name
        if final.resolve() in registered:
            continue
        match = re.match(r"^([0-9a-f-]{36})-", name)
        if match is None:
            database.record_system_event(
                "Recovery", "Warning", f"Unrecognised archive retained for inspection: {path}"
            )
            continue
        try:
            period = database.period(match[1])
        except KeyError:
            database.record_system_event(
                "Recovery", "Error", f"Archive has no known period: {path}"
            )
            continue
        if period["state"] not in {"OPEN", "DUE", "SEALED"}:
            database.record_system_event(
                "Recovery",
                "Error",
                f"Unregistered archive for an already reported period retained: {path}",
                period["id"],
            )
            continue
        database.open_raw_segment(period["id"], final)
        registered.add(final.resolve())

    segments = list(
        database.connection.execute(
            "SELECT raw_segments.*, report_periods.state AS period_state FROM raw_segments JOIN report_periods ON report_periods.id = raw_segments.report_period_id"
        )
    )
    for segment in segments:
        if segment["period_state"] == ReportPeriodState.FILES_DELETED.value:
            continue
        path = Path(segment["path"])
        try:
            path.resolve().relative_to(directory.resolve())
        except ValueError:
            raise RuntimeError(f"Archive outside managed records directory: {path}") from None
        partial = path.with_name(f".{path.stem}.partial.jsonl")
        canonical_count = database.connection.execute(
            "SELECT COUNT(*) FROM process_records WHERE raw_segment_id = ?", (segment["id"],)
        ).fetchone()[0]
        legacy = canonical_count == 0 and (path.exists() or partial.exists())
        if not legacy and canonical_count == 0 and segment["record_count"] > 0:
            raise RuntimeError(
                f"Legacy cycle data is unavailable; cannot rebuild an empty archive: {path}"
            )
        if legacy:
            source = path if path.exists() else partial
            if path.exists() and segment["sha256"] and archive_checksum(path) != segment["sha256"]:
                raise RuntimeError(f"Legacy archive checksum mismatch; retained untouched: {path}")
            count = 0
            for record in _salvage(source, database, segment["report_period_id"]):
                database.persist_record(
                    record, segment["report_period_id"], segment["id"], 0.0, increment_period=False
                )
                count += 1
            canonical_count = database.connection.execute(
                "SELECT COUNT(*) FROM process_records WHERE raw_segment_id = ?", (segment["id"],)
            ).fetchone()[0]
            if count != canonical_count:
                raise RuntimeError(f"Legacy archive contains duplicate identities: {source}")
        if (
            segment["closed_at"] is None
            or canonical_count != segment["record_count"]
            or not path.exists()
            or (segment["sha256"] and archive_checksum(path) != segment["sha256"])
        ):
            if path.exists() and segment["closed_at"] is not None:
                path.replace(path.with_name(f"{path.name}.{uuid.uuid4()}.quarantine"))
                database.record_system_event(
                    "Recovery",
                    "Warning",
                    f"Damaged mirror quarantined and rebuilt: {path}",
                    segment["report_period_id"],
                )
            checksum, count = rebuild_archive(path, database.segment_records(segment["id"]))
            database.register_raw_segment(segment["report_period_id"], path, checksum, count)
            if partial.exists():
                # Preserve legacy evidence, including any incomplete tail, for an operator.
                partial.replace(partial.with_name(f"{partial.name}.{uuid.uuid4()}.recovered"))

    for period in database.periods():
        if period["state"] in {"OPEN", "DUE", "SEALED"}:
            total = database.connection.execute(
                "SELECT COALESCE(SUM(record_count), 0) FROM raw_segments WHERE report_period_id = ?",
                (period["id"],),
            ).fetchone()[0]
            if total != period["record_count"]:
                detail = f"Record reconciliation failed: SQLite expects {period['record_count']}, archives contain {total}; original evidence retained"
                database.record_system_event("Recovery", "Error", detail, period["id"])
                database.transition_period(period["id"], ReportPeriodState.ERROR)


def _salvage(path: Path, database: AuditDatabase, period_id: str) -> Iterator[ProcessRecord]:
    if ".partial." not in path.name:
        yield from read_records(path)
        return
    with path.open("rb") as stream:
        for line in stream:
            if not line.endswith(b"\n"):
                database.record_system_event(
                    "Recovery",
                    "Error",
                    f"Incomplete legacy archive tail retained: {path}",
                    period_id,
                )
                break
            yield record_from_payload(json.loads(line))
