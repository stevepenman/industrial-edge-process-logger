"""SQLite state store with schema version tracking and transactional period sealing."""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from ..models import EmailState, ProcessRecord, ReportPeriodState
from ..state_machine import validate_transition
from .raw_archive import record_from_payload, record_identity, record_payload


class AuditDatabase:
    """Owns canonical cycle payloads and their transactional operational audit state."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.owner_thread_id = threading.get_ident()
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = FULL")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.execute("PRAGMA wal_autocheckpoint = 1000")
        self.migrate()

    def close(self) -> None:
        self._assert_owner()
        self.connection.close()

    def _assert_owner(self) -> None:
        """SQLite state is owned by the acquisition event-loop thread only."""
        if threading.get_ident() != self.owner_thread_id:
            raise RuntimeError("AuditDatabase may only be used by its creating thread")

    @staticmethod
    def read_only_check(path: Path) -> dict[str, object]:
        """Perform integrity checks without migration or any state mutation."""
        if not path.is_file():
            raise FileNotFoundError(f"SQLite state database does not exist: {path}")
        connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
        try:
            integrity = tuple(row[0] for row in connection.execute("PRAGMA integrity_check"))
            foreign_keys = tuple(
                tuple(row) for row in connection.execute("PRAGMA foreign_key_check")
            )
            return {
                "path": str(path),
                "integrity_check": integrity,
                "foreign_key_violations": foreign_keys,
                "valid": integrity == ("ok",) and not foreign_keys,
            }
        finally:
            connection.close()

    def durability_settings(self) -> dict[str, object]:
        self._assert_owner()
        return {
            "journal_mode": self.connection.execute("PRAGMA journal_mode").fetchone()[0],
            "synchronous": self.connection.execute("PRAGMA synchronous").fetchone()[0],
            "busy_timeout_ms": self.connection.execute("PRAGMA busy_timeout").fetchone()[0],
            "wal_autocheckpoint_pages": self.connection.execute(
                "PRAGMA wal_autocheckpoint"
            ).fetchone()[0],
            "foreign_keys": self.connection.execute("PRAGMA foreign_keys").fetchone()[0],
            "connection_owner_thread_id": self.owner_thread_id,
        }

    def migrate(self) -> None:
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        applied = {
            row[0] for row in self.connection.execute("SELECT version FROM schema_migrations")
        }
        if applied and 6 not in applied:
            backup = self.path.with_name(f"{self.path.name}.pre-v6-backup")
            if not backup.exists():
                with sqlite3.connect(backup) as destination:
                    self.connection.backup(destination)
        if 1 not in applied:
            self.connection.executescript(
                """
                BEGIN IMMEDIATE;
                    CREATE TABLE application_state (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE report_periods (
                        id TEXT PRIMARY KEY,
                        report_id TEXT NOT NULL UNIQUE,
                        state TEXT NOT NULL,
                        started_at TEXT NOT NULL,
                        sealed_at TEXT,
                        accumulated_seconds REAL NOT NULL DEFAULT 0,
                        record_count INTEGER NOT NULL DEFAULT 0,
                        workbook_path TEXT,
                        workbook_sha256 TEXT,
                        email_sent_at TEXT,
                        files_deleted_at TEXT,
                        error_detail TEXT
                    );
                    CREATE TABLE source_files (
                        sha256 TEXT PRIMARY KEY,
                        original_name TEXT NOT NULL,
                        size_bytes INTEGER NOT NULL,
                        imported_at TEXT NOT NULL
                    );
                    CREATE TABLE raw_segments (
                        id TEXT PRIMARY KEY,
                        report_period_id TEXT NOT NULL REFERENCES report_periods(id),
                        path TEXT NOT NULL,
                        sha256 TEXT,
                        record_count INTEGER NOT NULL DEFAULT 0,
                        opened_at TEXT NOT NULL,
                        closed_at TEXT
                    );
                    CREATE TABLE system_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        occurred_at TEXT NOT NULL,
                        category TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        detail TEXT NOT NULL,
                        report_period_id TEXT REFERENCES report_periods(id)
                    );
                    INSERT INTO schema_migrations VALUES (1, strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'));
                COMMIT;
                """
            )
            with self.transaction():
                self.connection.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (1, _now()),
                )
        if 2 not in applied:
            with self.transaction():
                self.connection.execute("ALTER TABLE source_files ADD COLUMN archive_path TEXT")
                self.connection.execute("ALTER TABLE source_files ADD COLUMN report_period_id TEXT")
                self.connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", (2, _now())
                )
        if 3 not in applied:
            self.connection.executescript(
                """
                BEGIN IMMEDIATE;
                    CREATE TABLE email_outbox (
                        report_period_id TEXT PRIMARY KEY REFERENCES report_periods(id),
                        state TEXT NOT NULL,
                        message_id TEXT NOT NULL UNIQUE,
                        recipients_json TEXT NOT NULL,
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        attempted_at TEXT,
                        sent_at TEXT,
                        response_detail TEXT,
                        updated_at TEXT NOT NULL
                    );
                    INSERT INTO schema_migrations VALUES (3, strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'));
                COMMIT;
                """
            )
            with self.transaction():
                self.connection.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (3, _now()),
                )
        if 4 not in applied:
            self.connection.executescript(
                """
                BEGIN IMMEDIATE;
                    CREATE TABLE report_period_sources (
                        report_period_id TEXT NOT NULL REFERENCES report_periods(id),
                        source_sha256 TEXT NOT NULL REFERENCES source_files(sha256),
                        PRIMARY KEY(report_period_id, source_sha256)
                    );
                    INSERT INTO schema_migrations VALUES (4, strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'));
                COMMIT;
                """
            )
            with self.transaction():
                self.connection.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (4, _now()),
                )
        if 5 not in applied:
            with self.transaction():
                self.connection.execute("ALTER TABLE source_files ADD COLUMN deleted_at TEXT")
                self.connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", (5, _now())
                )
        if 6 not in applied:
            with self.transaction():
                self.connection.execute("""CREATE TABLE process_records (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id TEXT NOT NULL UNIQUE,
                    report_period_id TEXT NOT NULL REFERENCES report_periods(id),
                    raw_segment_id TEXT NOT NULL REFERENCES raw_segments(id),
                    machine_name TEXT NOT NULL, run_id TEXT, cycle_id TEXT,
                    source_timestamp TEXT, received_timestamp TEXT NOT NULL,
                    source_sha256 TEXT, payload_json TEXT NOT NULL,
                    acquisition_seconds REAL NOT NULL DEFAULT 0
                )""")
                self.connection.execute(
                    "CREATE INDEX process_period ON process_records(report_period_id, sequence)"
                )
                self.connection.execute(
                    "CREATE INDEX process_cycle ON process_records(machine_name, run_id, cycle_id)"
                )
                self.connection.execute(
                    "ALTER TABLE source_files ADD COLUMN import_state TEXT NOT NULL DEFAULT 'IN_PROGRESS'"
                )
                self.connection.execute(
                    "ALTER TABLE source_files ADD COLUMN imported_records INTEGER NOT NULL DEFAULT 0"
                )
                self.connection.execute(
                    "ALTER TABLE report_periods ADD COLUMN storage_acknowledged_at TEXT"
                )
                self.connection.execute(
                    "ALTER TABLE report_periods ADD COLUMN storage_receipt TEXT"
                )
                self.connection.execute("ALTER TABLE email_outbox ADD COLUMN next_attempt_at TEXT")
                self.connection.execute("INSERT INTO schema_migrations VALUES (?, ?)", (6, _now()))
        if 7 not in applied:
            with self.transaction():
                self.connection.execute(
                    "CREATE INDEX IF NOT EXISTS process_segment ON process_records(raw_segment_id, sequence)"
                )
                self.connection.execute(
                    "CREATE INDEX IF NOT EXISTS process_machine_sequence ON process_records(machine_name, sequence)"
                )
                self.connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS one_open_period ON report_periods(state) WHERE state = 'OPEN'"
                )
                self.connection.execute("INSERT INTO schema_migrations VALUES (?, ?)", (7, _now()))

    def open_raw_segment(self, period_id: str, path: Path) -> str:
        segment_id = path.stem
        with self.transaction():
            self.connection.execute(
                "INSERT INTO raw_segments(id, report_period_id, path, opened_at) VALUES (?, ?, ?, ?)",
                (segment_id, period_id, str(path), _now()),
            )
        return segment_id

    def persist_record(
        self,
        record: ProcessRecord,
        period_id: str,
        segment_id: str,
        acquisition_seconds: float,
        source_checksum: str | None = None,
        increment_period: bool = True,
    ) -> bool:
        """Commit the canonical payload, deduplication key and counters together."""
        if acquisition_seconds < 0:
            raise ValueError("Acquisition time cannot decrease")
        payload = record_payload(record)
        with self.transaction():
            cursor = self.connection.execute(
                """INSERT OR IGNORE INTO process_records
                   (record_id, report_period_id, raw_segment_id, machine_name, run_id, cycle_id,
                    source_timestamp, received_timestamp, source_sha256, payload_json, acquisition_seconds)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    record_identity(record, source_checksum),
                    period_id,
                    segment_id,
                    record.machine_name,
                    record.run_id,
                    record.cycle_id,
                    payload["source_timestamp"],
                    payload["received_timestamp"],
                    source_checksum,
                    json.dumps(payload, separators=(",", ":"), default=str),
                    acquisition_seconds,
                ),
            )
            inserted = cursor.rowcount == 1
            if inserted:
                if increment_period:
                    self.connection.execute(
                        "UPDATE report_periods SET record_count = record_count + 1, accumulated_seconds = accumulated_seconds + ? WHERE id = ?",
                        (acquisition_seconds, period_id),
                    )
                if increment_period:
                    self.connection.execute(
                        "UPDATE raw_segments SET record_count = record_count + 1 WHERE id = ?",
                        (segment_id,),
                    )
                if source_checksum and increment_period:
                    self.connection.execute(
                        "UPDATE source_files SET imported_records = imported_records + 1 WHERE sha256 = ?",
                        (source_checksum,),
                    )
            return inserted

    def has_record(self, record: ProcessRecord, source_checksum: str | None = None) -> bool:
        return (
            self.connection.execute(
                "SELECT 1 FROM process_records WHERE record_id = ?",
                (record_identity(record, source_checksum),),
            ).fetchone()
            is not None
        )

    def segment_records(self, segment_id: str) -> Iterator[ProcessRecord]:
        for row in self.connection.execute(
            "SELECT payload_json FROM process_records WHERE raw_segment_id = ? ORDER BY sequence",
            (segment_id,),
        ):
            yield record_from_payload(json.loads(row[0]))

    def period_records(self, period_id: str) -> Iterator[tuple[str, str]]:
        """Stable machine-readable export, including the receiver's idempotency key."""
        for row in self.connection.execute(
            "SELECT record_id, payload_json FROM process_records WHERE report_period_id = ? ORDER BY sequence",
            (period_id,),
        ):
            yield str(row[0]), str(row[1])

    def acknowledge_storage(
        self, report_id: str, checksum: str, receipt: str, records_checksum: str | None = None
    ) -> None:
        period = self.report_period(report_id)
        if not receipt.strip() or checksum != period["workbook_sha256"] or not checksum:
            raise ValueError(
                "Storage acknowledgement requires the exact report SHA-256 and a receipt"
            )
        export = self.application_state(f"process_export:{period['id']}")
        if export and json.loads(export)["sha256"] != records_checksum:
            raise ValueError("Storage acknowledgement requires the exact process export SHA-256")
        with self.transaction():
            self.connection.execute(
                "UPDATE report_periods SET storage_acknowledged_at = ?, storage_receipt = ? WHERE id = ?",
                (_now(), receipt, period["id"]),
            )

    def complete_import(self, checksum: str) -> None:
        with self.transaction():
            self.connection.execute(
                "UPDATE source_files SET import_state = 'COMPLETE' WHERE sha256 = ?", (checksum,)
            )

    def import_complete(self, checksum: str) -> bool:
        row = self.connection.execute(
            "SELECT import_state FROM source_files WHERE sha256 = ?", (checksum,)
        ).fetchone()
        return row is not None and row[0] == "COMPLETE"

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self._assert_owner()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def get_or_create_open_period(self) -> sqlite3.Row:
        existing = self.connection.execute(
            "SELECT * FROM report_periods WHERE state = ? ORDER BY started_at LIMIT 1",
            (ReportPeriodState.OPEN.value,),
        ).fetchone()
        if existing is not None:
            return cast(sqlite3.Row, existing)
        return self._create_open_period()

    def open_period(self) -> sqlite3.Row | None:
        """Return the current period without creating state for read-only status."""
        return cast(
            sqlite3.Row | None,
            self.connection.execute(
                "SELECT * FROM report_periods WHERE state = ? ORDER BY started_at LIMIT 1",
                (ReportPeriodState.OPEN.value,),
            ).fetchone(),
        )

    def _create_open_period(self) -> sqlite3.Row:
        period_id = str(uuid.uuid4())
        report_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"demo-report-period:{period_id}"))
        self.connection.execute(
            "INSERT INTO report_periods(id, report_id, state, started_at) VALUES (?, ?, ?, ?)",
            (period_id, report_id, ReportPeriodState.OPEN.value, _now()),
        )
        return cast(
            sqlite3.Row,
            self.connection.execute(
                "SELECT * FROM report_periods WHERE id = ?", (period_id,)
            ).fetchone(),
        )

    def add_acquisition_seconds(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("Acquisition runtime cannot decrease")
        period = self.get_or_create_open_period()
        self.connection.execute(
            "UPDATE report_periods SET accumulated_seconds = accumulated_seconds + ? WHERE id = ?",
            (seconds, period["id"]),
        )

    def increment_record_count(self, count: int = 1) -> None:
        if count < 0:
            raise ValueError("Record count cannot decrease")
        period = self.get_or_create_open_period()
        self.connection.execute(
            "UPDATE report_periods SET record_count = record_count + ? WHERE id = ?",
            (count, period["id"]),
        )

    def record_acquisition(self, record_count: int, acquisition_seconds: float) -> sqlite3.Row:
        """Persist an ingestion batch against exactly one currently open period."""
        if record_count < 0 or acquisition_seconds < 0:
            raise ValueError("Record counts and acquisition runtime cannot decrease")
        with self.transaction():
            period = self.get_or_create_open_period()
            self.connection.execute(
                """
                UPDATE report_periods
                SET record_count = record_count + ?, accumulated_seconds = accumulated_seconds + ?
                WHERE id = ?
                """,
                (record_count, acquisition_seconds, period["id"]),
            )
            updated = self.connection.execute(
                "SELECT * FROM report_periods WHERE id = ?", (period["id"],)
            ).fetchone()
            assert updated is not None
            return cast(sqlite3.Row, updated)

    def mark_due_period(self, threshold_seconds: float) -> sqlite3.Row | None:
        """Persist DUE without starting work, for a shutdown-safe report trigger."""
        with self.transaction():
            period = self.get_or_create_open_period()
            if period["accumulated_seconds"] < threshold_seconds:
                return None
            self.connection.execute(
                "UPDATE report_periods SET state = ? WHERE id = ?",
                (ReportPeriodState.DUE.value, period["id"]),
            )
            return cast(
                sqlite3.Row | None,
                self.connection.execute(
                    "SELECT * FROM report_periods WHERE id = ?", (period["id"],)
                ).fetchone(),
            )

    def seal_due_period(self, threshold_seconds: float) -> sqlite3.Row | None:
        """Atomically marks one due period sealed and creates its successor."""
        with self.transaction():
            period = self.connection.execute(
                "SELECT * FROM report_periods WHERE state = ? ORDER BY started_at LIMIT 1",
                (ReportPeriodState.DUE.value,),
            ).fetchone()
            if period is None:
                period = self.get_or_create_open_period()
                if period["accumulated_seconds"] < threshold_seconds:
                    return None
                self.connection.execute(
                    "UPDATE report_periods SET state = ? WHERE id = ?",
                    (ReportPeriodState.DUE.value, period["id"]),
                )
            self.connection.execute(
                "UPDATE report_periods SET state = ?, sealed_at = ? WHERE id = ?",
                (ReportPeriodState.SEALED.value, _now(), period["id"]),
            )
            if (
                self.connection.execute(
                    "SELECT 1 FROM report_periods WHERE state = ?", (ReportPeriodState.OPEN.value,)
                ).fetchone()
                is None
            ):
                self._create_open_period()
            return cast(
                sqlite3.Row | None,
                self.connection.execute(
                    "SELECT * FROM report_periods WHERE id = ?", (period["id"],)
                ).fetchone(),
            )

    def force_seal_open_period(self) -> sqlite3.Row | None:
        """Seal non-empty current data for a deliberate test/replay report."""
        with self.transaction():
            period = self.get_or_create_open_period()
            if period["record_count"] == 0:
                return None
            self.connection.execute(
                "UPDATE report_periods SET state = ? WHERE id = ?",
                (ReportPeriodState.DUE.value, period["id"]),
            )
            self.connection.execute(
                "UPDATE report_periods SET state = ?, sealed_at = ? WHERE id = ?",
                (ReportPeriodState.SEALED.value, _now(), period["id"]),
            )
            self._create_open_period()
            return cast(
                sqlite3.Row | None,
                self.connection.execute(
                    "SELECT * FROM report_periods WHERE id = ?", (period["id"],)
                ).fetchone(),
            )

    def transition_period(self, period_id: str, target: ReportPeriodState) -> None:
        with self.transaction():
            period = self.connection.execute(
                "SELECT state FROM report_periods WHERE id = ?", (period_id,)
            ).fetchone()
            if period is None:
                raise KeyError(period_id)
            current = ReportPeriodState(period["state"])
            validate_transition(current, target)
            self.connection.execute(
                "UPDATE report_periods SET state = ? WHERE id = ?", (target.value, period_id)
            )

    def periods(self) -> list[sqlite3.Row]:
        return list(self.connection.execute("SELECT * FROM report_periods ORDER BY started_at"))

    def period(self, period_id: str) -> sqlite3.Row:
        period = self.connection.execute(
            "SELECT * FROM report_periods WHERE id = ?", (period_id,)
        ).fetchone()
        if period is None:
            raise KeyError(period_id)
        return cast(sqlite3.Row, period)

    def report_period(self, report_id: str) -> sqlite3.Row:
        period = self.connection.execute(
            "SELECT * FROM report_periods WHERE report_id = ?", (report_id,)
        ).fetchone()
        if period is None:
            raise KeyError(report_id)
        return cast(sqlite3.Row, period)

    def pending_periods(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                "SELECT * FROM report_periods WHERE state IN (?, ?) ORDER BY sealed_at",
                (ReportPeriodState.DUE.value, ReportPeriodState.SEALED.value),
            )
        )

    def report_counts_by_state(self) -> dict[str, int]:
        self._assert_owner()
        return {
            str(row["state"]): int(row["count"])
            for row in self.connection.execute(
                "SELECT state, COUNT(*) AS count FROM report_periods GROUP BY state"
            )
        }

    def email_counts_by_state(self) -> dict[str, int]:
        self._assert_owner()
        return {
            str(row["state"]): int(row["count"])
            for row in self.connection.execute(
                "SELECT state, COUNT(*) AS count FROM email_outbox GROUP BY state"
            )
        }

    def reset_interrupted_report_builds(self) -> list[sqlite3.Row]:
        """Return incomplete builds to SEALED so retained raw data can rebuild them."""
        with self.transaction():
            rows = list(
                self.connection.execute(
                    "SELECT * FROM report_periods WHERE state = ?",
                    (ReportPeriodState.REPORT_BUILDING.value,),
                )
            )
            for row in rows:
                self.connection.execute(
                    "UPDATE report_periods SET state = ?, error_detail = ? WHERE id = ?",
                    (
                        ReportPeriodState.SEALED.value,
                        "Recovered interrupted workbook build",
                        row["id"],
                    ),
                )
            return rows

    def recover_interrupted_email_sends(self) -> int:
        """A crash during SMTP delivery is ambiguous and must not auto-resend."""
        with self.transaction():
            rows = list(
                self.connection.execute(
                    "SELECT report_period_id FROM email_outbox WHERE state = ?",
                    (EmailState.SENDING.value,),
                )
            )
            for row in rows:
                period_id = str(row["report_period_id"])
                self.connection.execute(
                    "UPDATE email_outbox SET state = ?, response_detail = ?, updated_at = ? WHERE report_period_id = ?",
                    (
                        EmailState.AMBIGUOUS.value,
                        "Recovered interrupted SMTP attempt",
                        _now(),
                        period_id,
                    ),
                )
                self.connection.execute(
                    "UPDATE report_periods SET state = ? WHERE id = ?",
                    (ReportPeriodState.EMAIL_AMBIGUOUS.value, period_id),
                )
            return len(rows)

    def register_raw_segment(
        self, period_id: str, path: Path, checksum: str, record_count: int
    ) -> str:
        segment_id = str(uuid.uuid4())
        with self.transaction():
            self.period(period_id)
            existing = self.connection.execute(
                "SELECT id FROM raw_segments WHERE path = ? AND report_period_id = ?",
                (str(path), period_id),
            ).fetchone()
            if existing is not None:
                self.connection.execute(
                    "UPDATE raw_segments SET sha256 = ?, record_count = ?, closed_at = ? WHERE id = ?",
                    (checksum, record_count, _now(), existing["id"]),
                )
                return str(existing["id"])
            self.connection.execute(
                """
                INSERT INTO raw_segments(id, report_period_id, path, sha256, record_count, opened_at, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (segment_id, period_id, str(path), checksum, record_count, _now(), _now()),
            )
        return segment_id

    def raw_segments_for_period(self, period_id: str) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                "SELECT * FROM raw_segments WHERE report_period_id = ? ORDER BY opened_at",
                (period_id,),
            )
        )

    def source_files_for_period(self, period_id: str) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT source_files.* FROM source_files
                JOIN report_period_sources ON report_period_sources.source_sha256 = source_files.sha256
                WHERE report_period_sources.report_period_id = ? ORDER BY source_files.imported_at
                """,
                (period_id,),
            )
        )

    def associate_source_with_period(self, checksum: str, period_id: str) -> None:
        with self.transaction():
            self.period(period_id)
            self.connection.execute(
                "INSERT OR IGNORE INTO report_period_sources(report_period_id, source_sha256) VALUES (?, ?)",
                (period_id, checksum),
            )

    def source_can_be_deleted(self, checksum: str, cutoff: datetime) -> bool:
        rows = list(
            self.connection.execute(
                """
                SELECT report_periods.state, report_periods.email_sent_at, report_periods.storage_acknowledged_at
                FROM report_period_sources
                JOIN report_periods ON report_periods.id = report_period_sources.report_period_id
                WHERE report_period_sources.source_sha256 = ?
                """,
                (checksum,),
            )
        )
        if not rows:
            return False
        for row in rows:
            if row["storage_acknowledged_at"] is None:
                return False
            if row["state"] not in {
                ReportPeriodState.EMAIL_SENT.value,
                ReportPeriodState.RETENTION.value,
                ReportPeriodState.FILES_DELETED.value,
            }:
                return False
            if (
                row["email_sent_at"] is None
                or datetime.fromisoformat(row["email_sent_at"]) > cutoff
            ):
                return False
        return True

    def mark_source_deleted(self, checksum: str) -> None:
        with self.transaction():
            self.connection.execute(
                "UPDATE source_files SET deleted_at = ? WHERE sha256 = ?", (_now(), checksum)
            )

    def set_workbook(self, period_id: str, path: Path, checksum: str) -> None:
        with self.transaction():
            self.connection.execute(
                "UPDATE report_periods SET workbook_path = ?, workbook_sha256 = ? WHERE id = ?",
                (str(path), checksum, period_id),
            )

    def queue_email(self, period_id: str, recipients_json: str) -> sqlite3.Row:
        period = self.period(period_id)
        message_id = f"<{period['report_id']}@edge-logger.local>"
        with self.transaction():
            self.connection.execute(
                """
                INSERT OR IGNORE INTO email_outbox(report_period_id, state, message_id, recipients_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (period_id, EmailState.PENDING.value, message_id, recipients_json, _now()),
            )
            self.connection.execute(
                "UPDATE report_periods SET state = ? WHERE id = ? AND state = ?",
                (
                    ReportPeriodState.EMAIL_PENDING.value,
                    period_id,
                    ReportPeriodState.REPORT_VALIDATED.value,
                ),
            )
            row = self.connection.execute(
                "SELECT * FROM email_outbox WHERE report_period_id = ?", (period_id,)
            ).fetchone()
            assert row is not None
            return cast(sqlite3.Row, row)

    def email_outbox(self, period_id: str) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            self.connection.execute(
                "SELECT * FROM email_outbox WHERE report_period_id = ?", (period_id,)
            ).fetchone(),
        )

    def pending_email_outbox(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                "SELECT * FROM email_outbox WHERE state IN (?, ?) AND (next_attempt_at IS NULL OR next_attempt_at <= ?) ORDER BY updated_at",
                (EmailState.PENDING.value, EmailState.FAILED_RETRYABLE.value, _now()),
            )
        )

    def begin_email_attempt(self, period_id: str, allow_ambiguous: bool = False) -> sqlite3.Row:
        with self.transaction():
            outbox = self.email_outbox(period_id)
            if outbox is None:
                raise KeyError(period_id)
            current = EmailState(outbox["state"])
            allowed = {EmailState.PENDING, EmailState.FAILED_RETRYABLE}
            if allow_ambiguous:
                allowed.update({EmailState.AMBIGUOUS, EmailState.FAILED_PERMANENT})
            if current not in allowed:
                raise ValueError(f"Email state {current} is not eligible for send")
            self.connection.execute(
                "UPDATE email_outbox SET state = ?, attempt_count = attempt_count + 1, attempted_at = ?, updated_at = ? WHERE report_period_id = ?",
                (EmailState.SENDING.value, _now(), _now(), period_id),
            )
            self.connection.execute(
                "UPDATE report_periods SET state = ? WHERE id = ?",
                (ReportPeriodState.EMAIL_SENDING.value, period_id),
            )
            row = self.email_outbox(period_id)
            assert row is not None
            return row

    def finish_email_attempt(
        self, period_id: str, state: EmailState, detail: str | None = None
    ) -> None:
        if state not in {
            EmailState.SENT,
            EmailState.FAILED_RETRYABLE,
            EmailState.FAILED_PERMANENT,
            EmailState.AMBIGUOUS,
        }:
            raise ValueError(f"Terminal email result required, got {state}")
        report_state = {
            EmailState.SENT: ReportPeriodState.EMAIL_SENT,
            EmailState.FAILED_RETRYABLE: ReportPeriodState.EMAIL_PENDING,
            EmailState.FAILED_PERMANENT: ReportPeriodState.ERROR,
            EmailState.AMBIGUOUS: ReportPeriodState.EMAIL_AMBIGUOUS,
        }[state]
        with self.transaction():
            sent_at = _now() if state is EmailState.SENT else None
            outbox = self.email_outbox(period_id)
            attempts = int(outbox["attempt_count"]) if outbox is not None else 1
            next_attempt = (
                (
                    datetime.now(UTC) + timedelta(seconds=min(3600, 30 * 2 ** min(attempts - 1, 7)))
                ).isoformat()
                if state is EmailState.FAILED_RETRYABLE
                else None
            )
            self.connection.execute(
                "UPDATE email_outbox SET state = ?, sent_at = COALESCE(?, sent_at), response_detail = ?, updated_at = ?, next_attempt_at = ? WHERE report_period_id = ?",
                (state.value, sent_at, detail, _now(), next_attempt, period_id),
            )
            self.connection.execute(
                "UPDATE report_periods SET state = ?, email_sent_at = COALESCE(?, email_sent_at) WHERE id = ?",
                (report_state.value, sent_at, period_id),
            )

    def mark_files_deleted(self, period_id: str) -> None:
        with self.transaction():
            self.connection.execute(
                "UPDATE report_periods SET state = ?, files_deleted_at = ? WHERE id = ?",
                (ReportPeriodState.FILES_DELETED.value, _now(), period_id),
            )

    def record_system_event(
        self, category: str, severity: str, detail: str, report_period_id: str | None = None
    ) -> None:
        with self.transaction():
            self.connection.execute(
                """
                INSERT INTO system_events(occurred_at, category, severity, detail, report_period_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (_now(), category, severity, detail, report_period_id),
            )

    def set_application_state(self, key: str, value: str) -> None:
        with self.transaction():
            self.connection.execute(
                """
                INSERT INTO application_state(key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (key, value, _now()),
            )

    def application_state(self, key: str) -> str | None:
        row = self.connection.execute(
            "SELECT value FROM application_state WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row["value"])

    def register_source_file(
        self,
        checksum: str,
        original_name: str,
        size_bytes: int,
        archive_path: Path | None = None,
        report_period_id: str | None = None,
        import_state: str = "COMPLETE",
    ) -> bool:
        """Return False for an already-imported byte-identical source file."""
        with self.transaction():
            existing = self.connection.execute(
                "SELECT 1 FROM source_files WHERE sha256 = ?", (checksum,)
            ).fetchone()
            if existing is not None:
                return False
            self.connection.execute(
                """
                INSERT INTO source_files(sha256, original_name, size_bytes, imported_at, archive_path, report_period_id, import_state)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    checksum,
                    original_name,
                    size_bytes,
                    _now(),
                    str(archive_path) if archive_path else None,
                    report_period_id,
                    import_state,
                ),
            )
            return True


def _now() -> str:
    return datetime.now(UTC).isoformat()
