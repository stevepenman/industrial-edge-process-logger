"""Audited retention execution for files proven safe to remove."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from ..models import ReportPeriodState
from ..storage.audit_db import AuditDatabase
from ..storage.raw_archive import archive_checksum
from .cleanup import eligible_for_deletion


@dataclass(frozen=True, slots=True)
class RetentionResult:
    report_id: str
    eligible: bool
    deleted_paths: tuple[Path, ...]
    reason: str


def run_retention(
    database: AuditDatabase,
    reports_path: Path,
    raw_path: Path,
    retention_days: int,
    now: datetime,
    dry_run: bool = True,
) -> tuple[RetentionResult, ...]:
    """Delete only validated, successfully emailed report files and raw segments."""
    results: list[RetentionResult] = []
    for period in database.periods():
        state = ReportPeriodState(str(period["state"]))
        sent_at = (
            datetime.fromisoformat(period["email_sent_at"]) if period["email_sent_at"] else None
        )
        if not eligible_for_deletion(state, sent_at, now, retention_days):
            results.append(
                RetentionResult(str(period["report_id"]), False, (), "Not confirmed-email eligible")
            )
            continue
        if not period["storage_acknowledged_at"]:
            results.append(
                RetentionResult(
                    str(period["report_id"]), False, (), "Durable storage acknowledgement required"
                )
            )
            continue
        sources = database.source_files_for_period(str(period["id"]))
        if any(row["import_state"] != "COMPLETE" for row in sources):
            results.append(
                RetentionResult(str(period["report_id"]), False, (), "Source import is incomplete")
            )
            continue
        paths = [Path(str(period["workbook_path"]))] if period["workbook_path"] else []
        archives = database.raw_segments_for_period(str(period["id"]))
        paths.extend(Path(str(row["path"])) for row in archives)
        checksums = {str(row["path"]): row["sha256"] for row in archives}
        if period["workbook_path"]:
            checksums[str(period["workbook_path"])] = period["workbook_sha256"]
        export = database.application_state(f"process_export:{period['id']}")
        if export:
            metadata = json.loads(export)
            paths.append(Path(metadata["path"]))
            checksums[metadata["path"]] = metadata["sha256"]
        safe_paths = tuple(
            path for path in paths if _inside(path, reports_path) or _inside(path, raw_path)
        )
        if len(safe_paths) != len(paths):
            results.append(
                RetentionResult(
                    str(period["report_id"]), False, (), "Refused path outside managed storage"
                )
            )
            continue
        missing = (
            tuple(path for path in safe_paths if not path.is_file())
            if state is not ReportPeriodState.RETENTION
            else ()
        )
        if missing:
            detail = (
                f"Retention refused; audit-managed file is missing: {', '.join(map(str, missing))}"
            )
            database.record_system_event("Retention", "Error", detail, str(period["id"]))
            results.append(RetentionResult(str(period["report_id"]), False, (), detail))
            continue
        changed = tuple(
            path
            for path in safe_paths
            if path.is_file() and archive_checksum(path) != checksums.get(str(path))
        )
        if changed:
            detail = f"Retention refused; file checksum mismatch: {changed}"
            database.record_system_event("Retention", "Error", detail, str(period["id"]))
            results.append(RetentionResult(str(period["report_id"]), False, (), detail))
            continue
        if not dry_run:
            if state is ReportPeriodState.EMAIL_SENT:
                database.transition_period(str(period["id"]), ReportPeriodState.RETENTION)
            for path in safe_paths:
                path.unlink(missing_ok=True)
            _delete_eligible_source_files(
                database, str(period["id"]), raw_path, retention_days, now
            )
            database.mark_files_deleted(str(period["id"]))
        results.append(RetentionResult(str(period["report_id"]), True, safe_paths, "Eligible"))
    return tuple(results)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _delete_eligible_source_files(
    database: AuditDatabase, period_id: str, raw_path: Path, retention_days: int, now: datetime
) -> None:
    cutoff = now - timedelta(days=retention_days)
    for source in database.source_files_for_period(period_id):
        path_value = source["archive_path"]
        if not path_value or not database.source_can_be_deleted(str(source["sha256"]), cutoff):
            continue
        path = Path(str(path_value))
        if not _inside(path, raw_path):
            continue
        if path.exists():
            if archive_checksum(path) != source["sha256"]:
                raise RuntimeError(f"Source archive checksum mismatch; deletion refused: {path}")
            path.unlink()
        database.mark_source_deleted(str(source["sha256"]))
