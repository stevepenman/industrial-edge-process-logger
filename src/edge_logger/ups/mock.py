from __future__ import annotations

from .base import UpsProvider, UpsStatus


class MockUpsProvider(UpsProvider):
    def __init__(self, current: UpsStatus | None = None) -> None:
        self.current = current or UpsStatus(
            external_power_present=True, battery_critical=False, battery_percent=100
        )

    async def status(self) -> UpsStatus:
        return self.current
