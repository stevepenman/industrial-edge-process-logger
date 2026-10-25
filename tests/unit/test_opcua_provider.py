"""Synthetic fixtures only; no operational datasets are required."""

import asyncio
import inspect
import socket
from datetime import UTC, datetime
from types import SimpleNamespace

from edge_logger.acquisition.opcua import OpcUaConnectionSettings, OpcUaNode, OpcUaProvider
from edge_logger.models import QualityStatus
from tools.opcua_simulator import DevelopmentOpcUaSimulator


def _port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def test_provider_public_surface_has_no_write_or_control_methods() -> None:
    public = {
        name
        for name, member in inspect.getmembers(OpcUaProvider, predicate=callable)
        if not name.startswith("_")
    }
    assert {"records", "browse_root", "stop", "from_configuration"}.issubset(public)
    assert not {
        "write_value",
        "set_value",
        "write_attribute",
        "call_method",
        "control",
    }.intersection(public)
    assert not {
        name
        for name in public
        if any(token in name.lower() for token in ("write", "control", "method"))
    }


def test_data_change_preserves_source_server_receive_and_status() -> None:
    source = datetime(2026, 8, 20, 12, tzinfo=UTC)
    server = datetime(2026, 8, 20, 12, 0, 1, tzinfo=UTC)
    provider = OpcUaProvider(
        OpcUaConnectionSettings("opc.tcp://127.0.0.1:4840", queue_size=2),
        (
            OpcUaNode("pressure", "ns=2;i=1", "Pressure", "bar", "float"),
            OpcUaNode("cycle", "ns=2;i=2", "Cycle", None, "int", is_cycle_id=True),
        ),
    )
    pressure_data = SimpleNamespace(
        monitored_item=SimpleNamespace(
            Value=SimpleNamespace(SourceTimestamp=source, ServerTimestamp=server, StatusCode="Bad")
        )
    )
    cycle_data = SimpleNamespace(
        monitored_item=SimpleNamespace(
            Value=SimpleNamespace(SourceTimestamp=source, ServerTimestamp=server, StatusCode="Good")
        )
    )
    node = lambda value: SimpleNamespace(nodeid=SimpleNamespace(to_string=lambda: value))

    async def exercise() -> None:
        await provider._data_change(node("ns=2;i=1"), 100.0, pressure_data)
        await provider._data_change(node("ns=2;i=2"), 42, cycle_data)

    asyncio.run(exercise())
    record = provider._records.get_nowait()
    pressure = next(signal for signal in record.signals if signal.signal_id == "pressure")
    assert record.source_timestamp == source
    assert record.received_timestamp.tzinfo is UTC
    assert pressure.quality == QualityStatus.BAD
    assert pressure.server_timestamp == server and pressure.status_code == "Bad"


def test_local_simulator_proves_subscription_and_reconnect() -> None:
    endpoint = f"opc.tcp://127.0.0.1:{_port()}/demo-simulator/"

    async def exercise() -> None:
        async def receive_cycle(expected: str) -> object:
            for _ in range(20):
                record = await asyncio.wait_for(anext(stream), 5)
                if record.cycle_id == expected:
                    return record
            raise AssertionError(f"Did not receive cycle {expected}")

        simulator = DevelopmentOpcUaSimulator(endpoint)
        await simulator.start()
        try:
            provider = OpcUaProvider(
                OpcUaConnectionSettings(
                    endpoint,
                    subscription_interval_ms=50,
                    reconnect_initial_seconds=0.05,
                    reconnect_max_seconds=0.2,
                ),
                (
                    OpcUaNode(
                        "cycle",
                        simulator.node_ids["CycleId"],
                        "Cycle",
                        None,
                        "int",
                        is_cycle_id=True,
                    ),
                    OpcUaNode(
                        "cycle_time", simulator.node_ids["CycleTime"], "Cycle time", "s", "float"
                    ),
                    OpcUaNode("feed", simulator.node_ids["FeedTemperature"], "Feed", "C", "float"),
                ),
            )
            stream = provider.records()
            first_task = asyncio.create_task(anext(stream))
            await asyncio.sleep(0.2)
            await simulator.normal_cycle(1)
            first = await asyncio.wait_for(first_task, 5)
            if first.cycle_id != "1":
                first = await receive_cycle("1")
            assert first.cycle_id == "1"
            assert first.production_context["snapshot_complete"] is True
            assert (
                next(signal.value for signal in first.signals if signal.signal_id == "feed") == 51.0
            )
            await simulator.stop()
            await asyncio.sleep(0.2)
            await simulator.start()
            await asyncio.sleep(0.4)
            await simulator.normal_cycle(2)
            second = await receive_cycle("2")
            assert second.cycle_id == "2"
            assert second.production_context["snapshot_complete"] is True
            assert (
                next(signal.value for signal in second.signals if signal.signal_id == "feed")
                == 52.0
            )
            await provider.stop()
            await stream.aclose()
        finally:
            await simulator.stop()

    asyncio.run(exercise())
