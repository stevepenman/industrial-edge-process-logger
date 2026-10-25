"""Development-only DEMO-01-shaped OPC UA simulator.

This program is intentionally outside the production package and must never be installed
or enabled by the logger service. Its server-side writes exist only to create test signals.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
from typing import Any


class DevelopmentOpcUaSimulator:
    """Small local server for subscription, reconnect, and quality tests."""

    def __init__(self, endpoint: str = "opc.tcp://127.0.0.1:48410/demo-simulator/") -> None:
        self.endpoint = endpoint
        self.server: Any | None = None
        self.nodes: dict[str, Any] = {}
        self.node_ids: dict[str, str] = {}
        self.cycle = 0

    async def start(self) -> None:
        from asyncua import Server

        self.server = Server()
        await self.server.init()
        self.server.set_endpoint(self.endpoint)
        namespace = await self.server.register_namespace("urn:demo:development-simulator")
        machine = await self.server.nodes.objects.add_object(namespace, "DEMO-01Development")
        for name, value in _INITIAL_VALUES.items():
            node = await machine.add_variable(namespace, name, value)
            await node.set_writable()
            self.nodes[name] = node
            self.node_ids[name] = node.nodeid.to_string()
        await self.server.start()

    async def stop(self) -> None:
        if self.server is not None:
            await self.server.stop()
            self.server = None

    async def normal_cycle(self, cycle: int | None = None, long_cycle: bool = False) -> None:
        from asyncua import ua

        self.cycle = self.cycle + 1 if cycle is None else cycle
        completed_at = datetime.now(UTC)
        for name, value in {
            "CycleTime": 30.0 if long_cycle else 9.5,
            "FeedTemperature": 50.0 + self.cycle % 3,
            "InjectionPressure": 100.0 + self.cycle,
            "BadPart": bool(self.cycle % 7 == 0),
            "CycleId": self.cycle,
        }.items():
            data_value = ua.DataValue(ua.Variant(value))
            data_value.SourceTimestamp = completed_at
            await self.nodes[name].write_value(data_value)

    async def constant_values(self) -> None:
        for name in ("TCD01", "TCD02", "TCD03", "TCD04", "TCD05", "TCD06", "TCD07", "TCD08"):
            await self.nodes[name].write_value(42.0)

    async def bad_quality(self, value: float = 0.0) -> None:
        from asyncua import ua

        data_value = ua.DataValue(ua.Variant(value))
        data_value.SourceTimestamp = datetime.now(UTC)
        data_value.StatusCode = ua.StatusCode(ua.StatusCodes.BadNoData)
        await self.nodes["InjectionPressure"].write_value(data_value)


_INITIAL_VALUES = {
    "CycleId": 0,
    "CycleTime": 9.5,
    "FeedTemperature": 50.0,
    "BarrelTemperature1": 200.0,
    "BarrelTemperature2": 205.0,
    "NozzleTemperature": 210.0,
    "OilTemperature": 30.0,
    "TCD01": 42.0,
    "TCD02": 42.0,
    "TCD03": 42.0,
    "TCD04": 42.0,
    "TCD05": 42.0,
    "TCD06": 42.0,
    "TCD07": 42.0,
    "TCD08": 42.0,
    "InjectionPressure": 100.0,
    "MeltCushion": 3.0,
    "ScrewBackTime": 2.0,
    "BadPart": False,
}


async def _main(endpoint: str) -> None:
    simulator = DevelopmentOpcUaSimulator(endpoint)
    await simulator.start()
    try:
        while True:
            await simulator.normal_cycle()
            await asyncio.sleep(1)
    finally:
        await simulator.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Development-only DEMO-01 OPC UA simulator")
    parser.add_argument("--endpoint", default="opc.tcp://127.0.0.1:48410/demo-simulator/")
    arguments = parser.parse_args()
    asyncio.run(_main(arguments.endpoint))
