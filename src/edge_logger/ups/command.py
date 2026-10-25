"""Adapter for a site-verified UPS utility; no GPIO/register assignments are guessed."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from pathlib import Path

from ..storage.raw_archive import fsync_directory
from .base import UpsProvider, UpsStatus


class CommandUpsProvider(UpsProvider):
    def __init__(self, command: Sequence[str], timeout_seconds: float = 3) -> None:
        self.command = tuple(command)
        self.timeout_seconds = timeout_seconds

    async def status(self) -> UpsStatus:
        process = await asyncio.create_subprocess_exec(
            *self.command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, _ = await asyncio.wait_for(process.communicate(), self.timeout_seconds)
        except BaseException:
            process.kill()
            await process.wait()
            raise
        if process.returncode:
            raise RuntimeError(f"UPS status utility failed with exit code {process.returncode}")
        result = json.loads(stdout)
        for key in ("external_power_present", "battery_critical"):
            if type(result.get(key)) is not bool:
                raise ValueError(f"UPS status requires boolean {key}")
        percentage = result.get("battery_percent")
        if percentage is not None and (
            isinstance(percentage, bool)
            or not isinstance(percentage, (int, float))
            or not 0 <= percentage <= 100
        ):
            raise ValueError("UPS battery_percent must be between 0 and 100")
        return UpsStatus(result["external_power_present"], result["battery_critical"], percentage)


async def request_host_shutdown(state_directory: Path) -> None:
    """A separately installed root systemd path unit consumes this durable request."""
    import os

    path = state_directory / "shutdown-request.json"
    temporary = state_directory / ".shutdown-request.tmp"
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(json.dumps({"reason": "logger UPS shutdown"}))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fsync_directory(state_directory)
