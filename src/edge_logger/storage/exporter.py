"""Stream a sealed period through an independent read-only SQLite connection."""

from __future__ import annotations

import gzip
import json
import os
import sqlite3
import uuid
from pathlib import Path

from .raw_archive import archive_checksum, fsync_directory


def export_records(
    database_path: Path, directory: Path, period_id: str, report_id: str, expected_count: int
) -> dict[str, object]:
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"{report_id}.records.jsonl.gz"
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4()}.tmp")
    connection = sqlite3.connect(f"{database_path.resolve().as_uri()}?mode=ro", uri=True)
    count = 0
    try:
        with temporary.open("xb") as stream:
            with gzip.GzipFile(fileobj=stream, mode="wb", filename="", mtime=0) as compressed:
                for record_id, payload in connection.execute(
                    "SELECT record_id, payload_json FROM process_records WHERE report_period_id = ? ORDER BY sequence",
                    (period_id,),
                ):
                    compressed.write(
                        (
                            json.dumps(
                                {
                                    "schema_version": 1,
                                    "record_id": record_id,
                                    "record": json.loads(payload),
                                },
                                separators=(",", ":"),
                            )
                            + "\n"
                        ).encode()
                    )
                    count += 1
            stream.flush()
            os.fsync(stream.fileno())
        if count != expected_count:
            raise ValueError("Process export count does not match committed cycle count")
        os.replace(temporary, destination)
        fsync_directory(directory)
    finally:
        connection.close()
        temporary.unlink(missing_ok=True)
    return {
        "path": str(destination),
        "sha256": archive_checksum(destination),
        "record_count": count,
    }
