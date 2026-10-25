"""Immutable retention of original imported source files."""

from __future__ import annotations

import hashlib
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ArchivedSource:
    original_name: str
    archive_path: Path
    sha256: str
    size_bytes: int
    imported_at: datetime


def archive_source_file(source: Path, archive_directory: Path) -> ArchivedSource:
    """Copy source bytes once, checksum them, and atomically retain the copy."""
    if not source.is_file():
        raise FileNotFoundError(f"Source file does not exist: {source}")
    checksum = sha256_file(source)
    archive_directory.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(
        character for character in source.name if character.isalnum() or character in ".-_"
    )
    destination = archive_directory / f"{checksum}_{safe_name or 'source'}"
    if destination.exists():
        if sha256_file(destination) != checksum:
            raise RuntimeError(f"Existing source archive checksum mismatch: {destination}")
    else:
        temporary = archive_directory / f".{destination.name}.tmp"
        try:
            with source.open("rb") as input_stream, temporary.open("wb") as output_stream:
                shutil.copyfileobj(input_stream, output_stream, length=1024 * 1024)
                output_stream.flush()
                os.fsync(output_stream.fileno())
            if sha256_file(temporary) != checksum:
                raise RuntimeError("Copied source checksum does not match original source")
            os.replace(temporary, destination)
            _fsync_directory(archive_directory)
        finally:
            if temporary.exists():
                temporary.unlink()
    return ArchivedSource(
        original_name=source.name,
        archive_path=destination,
        sha256=checksum,
        size_bytes=source.stat().st_size,
        imported_at=datetime.now(UTC),
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(directory: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
