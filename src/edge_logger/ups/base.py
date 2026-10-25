"""UPS interface with no guessed board registers or GPIO mappings."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class UpsStatus:
    external_power_present: bool
    battery_critical: bool
    battery_percent: float | None = None


class UpsProvider(ABC):
    @abstractmethod
    async def status(self) -> UpsStatus:
        """Return the latest verified board status."""
