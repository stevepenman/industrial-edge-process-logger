"""Append-only JSONL raw archives, preserving exactly what acquisition supplied."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Self

from ..models import ProcessRecord, QualityStatus, SignalValue


class RawArchiveWriter:
    def __init__(self, directory: Path, segment_id: str) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / f"{segment_id}.jsonl"
        self._temporary_path = directory / f".{segment_id}.partial.jsonl"
        self._stream = self._temporary_path.open("x", encoding="utf-8", buffering=1024 * 1024)
        self.record_count = 0
        self.checksum: str | None = None

    def append(self, record: ProcessRecord) -> None:
        self._stream.write(
            json.dumps(record_payload(record), separators=(",", ":"), default=str) + "\n"
        )
        self.record_count += 1

    def checkpoint(self) -> None:
        self._stream.flush()
        os.fsync(self._stream.fileno())

    def close(self) -> str:
        if self.checksum is not None:
            return self.checksum
        self.checkpoint()
        self._stream.close()
        os.replace(self._temporary_path, self.path)
        fsync_directory(self.path.parent)
        self.checksum = archive_checksum(self.path)
        return self.checksum

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        if not self._stream.closed:
            self.close()


def record_payload(record: ProcessRecord) -> dict[str, object]:
    return {
        "machine_name": record.machine_name,
        "source_timestamp": _timestamp(record.source_timestamp),
        "received_timestamp": _timestamp(record.received_timestamp),
        "cycle_id": record.cycle_id,
        "source_identifier": record.source_identifier,
        "run_id": record.run_id,
        "production_context": record.production_context,
        "signals": [
            {
                "signal_id": signal.signal_id,
                "display_name": signal.display_name,
                "value": signal.value,
                "unit": signal.unit,
                "datatype": signal.datatype,
                "quality": signal.quality.value,
                "source_timestamp": _timestamp(signal.source_timestamp),
                "server_timestamp": _timestamp(signal.server_timestamp),
                "status_code": signal.status_code,
                "source_name": signal.source_name,
                "raw_value": signal.raw_value,
            }
            for signal in record.signals
        ],
    }


def _timestamp(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


def read_records(path: Path) -> Iterator[ProcessRecord]:
    """Stream records back from an immutable raw archive for report rebuilding."""
    with path.open("r", encoding="utf-8") as archive:
        for line in archive:
            payload = json.loads(line)
            yield record_from_payload(payload)


def record_from_payload(payload: dict) -> ProcessRecord:  # type: ignore[type-arg]
    received_timestamp = _parse_timestamp(payload["received_timestamp"])
    if received_timestamp is None:
        raise ValueError("Raw archive record is missing received_timestamp")
    return ProcessRecord(
        machine_name=payload["machine_name"],
        source_timestamp=_parse_timestamp(payload["source_timestamp"]),
        received_timestamp=received_timestamp,
        cycle_id=payload["cycle_id"],
        source_identifier=payload["source_identifier"],
        run_id=payload.get("run_id"),
        production_context=payload.get("production_context", {}),
        signals=tuple(
            SignalValue(
                signal_id=signal["signal_id"],
                display_name=signal["display_name"],
                value=signal["value"],
                unit=signal["unit"],
                datatype=signal["datatype"],
                quality=QualityStatus(signal["quality"]),
                source_timestamp=_parse_timestamp(signal["source_timestamp"]),
                server_timestamp=_parse_timestamp(signal.get("server_timestamp")),
                status_code=signal.get("status_code"),
                source_name=signal.get("source_name"),
                raw_value=signal.get("raw_value"),
            )
            for signal in payload["signals"]
        ),
    )


def record_identity(record: ProcessRecord, source_checksum: str | None = None) -> str:
    """Never use a resetting machine cycle counter as a global unique identifier."""
    identity: list[str | None]
    if record.run_id and record.cycle_id is not None:
        identity = ["cycle", record.machine_name, record.run_id, record.cycle_id]
    elif record.cycle_id is not None and record.source_timestamp is not None:
        identity = [
            "timestamped-cycle",
            record.machine_name,
            record.cycle_id,
            _timestamp(record.source_timestamp),
        ]
    elif source_checksum:
        identity = ["import", source_checksum, record.source_identifier.rsplit(":", 1)[-1]]
    else:
        identity = [
            "timestamped-cycle",
            record.machine_name,
            record.cycle_id,
            _timestamp(record.source_timestamp),
            record.source_identifier,
        ]
        if record.source_timestamp is None:
            identity.append(_timestamp(record.received_timestamp))
    return hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()


def archive_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as archive:
        for chunk in iter(lambda: archive.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def rebuild_archive(path: Path, records: Iterator[ProcessRecord]) -> tuple[str, int]:
    """Restore a mirror from committed SQLite records without trusting partial bytes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4()}.restore")
    count = 0
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            for record in records:
                stream.write(
                    json.dumps(record_payload(record), separators=(",", ":"), default=str) + "\n"
                )
                count += 1
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return archive_checksum(path), count


def fsync_directory(directory: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _parse_timestamp(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None
