"""Deterministic UPS outage timer, independent of unverified UPS hardware I/O."""

from __future__ import annotations

from dataclasses import dataclass

from .base import UpsStatus


@dataclass(slots=True)
class UpsShutdownController:
    shutdown_delay_seconds: float
    power_lost_at: float | None = None

    def observe(self, status: UpsStatus, monotonic_seconds: float) -> bool:
        if status.battery_critical:
            return True
        if status.external_power_present:
            self.power_lost_at = None
            return False
        if self.power_lost_at is None:
            self.power_lost_at = monotonic_seconds
            return False
        return monotonic_seconds - self.power_lost_at >= self.shutdown_delay_seconds
