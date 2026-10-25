"""Idempotent ingestion of emailed machine-readable exports on a receiving host."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from .raw_archive import archive_checksum, record_from_payload


def ingest_export(source: Path, database_path: Path) -> dict[str, object]:
    """Commit the whole export or none of it; return a durable storage receipt."""
    checksum = archive_checksum(source)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("""CREATE TABLE IF NOT EXISTS cycles (
            record_id TEXT PRIMARY KEY, machine_name TEXT NOT NULL, run_id TEXT,
            cycle_id TEXT, source_timestamp TEXT, received_timestamp TEXT NOT NULL,
            payload_json TEXT NOT NULL)""")
        connection.execute("""CREATE TABLE IF NOT EXISTS receipts (
            export_sha256 TEXT PRIMARY KEY, record_count INTEGER NOT NULL,
            receipt TEXT NOT NULL, stored_at TEXT NOT NULL)""")
        existing = connection.execute(
            "SELECT record_count, receipt FROM receipts WHERE export_sha256 = ?", (checksum,)
        ).fetchone()
        if existing:
            return {
                "export_sha256": checksum,
                "record_count": existing[0],
                "receipt": existing[1],
                "duplicate": True,
            }
        count = 0
        with connection, gzip.open(source, "rt", encoding="utf-8") as stream:
            while True:
                line = stream.readline(1024 * 1024 + 1)
                if not line:
                    break
                if len(line) > 1024 * 1024 or not line.endswith("\n"):
                    raise ValueError("Export contains an oversized or incomplete record")
                entry = json.loads(line)
                record_id = entry.get("record_id", "")
                if entry.get("schema_version") != 1 or not re.fullmatch("[0-9a-f]{64}", record_id):
                    raise ValueError("Unsupported export schema or record ID")
                payload = entry["record"]
                record = record_from_payload(payload)
                canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
                prior = connection.execute(
                    "SELECT payload_json FROM cycles WHERE record_id = ?", (record_id,)
                ).fetchone()
                if prior and prior[0] != canonical:
                    raise ValueError(
                        f"Conflicting payload for record {record_id}; export rolled back"
                    )
                connection.execute(
                    "INSERT OR IGNORE INTO cycles VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        record_id,
                        record.machine_name,
                        record.run_id,
                        record.cycle_id,
                        payload["source_timestamp"],
                        payload["received_timestamp"],
                        canonical,
                    ),
                )
                count += 1
            receipt = hashlib.sha256(f"demo-storage-v1:{checksum}:{count}".encode()).hexdigest()
            connection.execute(
                "INSERT INTO receipts VALUES (?, ?, ?, ?)",
                (checksum, count, receipt, datetime.now(UTC).isoformat()),
            )
        return {
            "export_sha256": checksum,
            "record_count": count,
            "receipt": receipt,
            "duplicate": False,
        }
    finally:
        connection.close()
