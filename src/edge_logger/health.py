"""Low-frequency, read-only host health snapshot collection."""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    cpu_temperature_c: float | None
    thermal_throttled: bool | None
    disk_free_bytes: int
    disk_used_percent: float
    ntp_synchronized: bool | None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def collect_health(storage_path: Path) -> HealthSnapshot:
    disk_path = _existing_parent(storage_path)
    usage = shutil.disk_usage(disk_path)
    return HealthSnapshot(
        _cpu_temperature(),
        _thermal_throttled(),
        usage.free,
        round(usage.used / usage.total * 100, 2),
        _ntp_synchronized(),
    )


def _cpu_temperature() -> float | None:
    temperature_file = Path("/sys/class/thermal/thermal_zone0/temp")
    try:
        return int(temperature_file.read_text(encoding="ascii").strip()) / 1000
    except (OSError, ValueError):
        return None


def _ntp_synchronized() -> bool | None:
    if os.name != "posix":
        return None
    try:
        result = subprocess.run(
            ["timedatectl", "show", "--property=NTPSynchronized", "--value"],
            capture_output=True,
            check=True,
            text=True,
            timeout=2,
        )
        value = result.stdout.strip().lower()
        return True if value == "yes" else False if value == "no" else None
    except (OSError, subprocess.SubprocessError):
        return None


def _thermal_throttled() -> bool | None:
    """Read only the current Raspberry Pi throttling bits when vcgencmd exists."""
    try:
        result = subprocess.run(
            ["vcgencmd", "get_throttled"], capture_output=True, check=True, text=True, timeout=1
        )
        value = int(result.stdout.strip().split("=", maxsplit=1)[1], 16)
        return bool(value & 0xC)
    except (FileNotFoundError, IndexError, subprocess.SubprocessError, ValueError):
        return None


def _existing_parent(path: Path) -> Path:
    current = path
    while not current.exists() and current != current.parent:
        current = current.parent
    return current
