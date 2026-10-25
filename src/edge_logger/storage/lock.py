"""Prevent concurrent acquisition/import processes from sharing active archives."""

from __future__ import annotations

import sys
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Self


class AcquisitionLock:
    def __init__(self, directory: Path) -> None:
        self.path = directory / "acquisition.lock"
        self.stream: BinaryIO | None = None

    def __enter__(self) -> Self:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("a+b")
        try:
            if sys.platform == "win32":
                import msvcrt

                self.stream.seek(0)
                if not self.stream.read(1):
                    self.stream.write(b"0")
                    self.stream.flush()
                self.stream.seek(0)
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self.stream.close()
            self.stream = None
            raise RuntimeError(
                "Another logger run/import process owns acquisition state"
            ) from error
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self.stream is not None:
            self.stream.close()
            self.stream = None
