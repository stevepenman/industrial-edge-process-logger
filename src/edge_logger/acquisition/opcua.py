"""Read-only asyncua subscription provider.

The wrapper intentionally exposes record acquisition, root browsing, and shutdown only.
It does not surface asyncua's write or machine-method capabilities.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

from ..models import ProcessRecord, QualityStatus, SignalValue
from .base import AcquisitionProvider

LOG = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class OpcUaConnectionSettings:
    endpoint: str
    security_policy: str | None = None
    security_mode: str | None = None
    username: str | None = None
    password: str | None = None
    client_certificate: str | None = None
    private_key: str | None = None
    trusted_server_certificate: str | None = None
    subscription_interval_ms: int = 1_000
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    queue_size: int = 1_000
    monitored_item_queue_size: int = 1_000
    snapshot_wait_seconds: float = 2.0
    snapshot_timestamp_tolerance_seconds: float = 0.0
    asynchronous_max_age_seconds: float = 60.0
    run_id: str | None = None


@dataclass(frozen=True, slots=True)
class OpcUaNode:
    signal_id: str
    node_id: str
    display_name: str
    unit: str | None
    datatype: str | None
    is_cycle_id: bool = False
    required: bool = False
    cycle_synchronous: bool = True
    is_running_state: bool = False
    production_field: str | None = None


@dataclass(frozen=True, slots=True)
class _PendingCycle:
    signal: SignalValue
    received: datetime
    expires: float


class OpcUaProvider(AcquisitionProvider):
    """Reconnectable, read-only data-change subscriber for OEM-configured NodeIds."""

    def __init__(
        self,
        settings: OpcUaConnectionSettings,
        nodes: tuple[OpcUaNode, ...] = (),
        machine_name: str = "DEMO-01",
    ) -> None:
        self.settings = settings
        self.nodes = nodes
        self.machine_name = machine_name
        self._stop_event = asyncio.Event()
        self._records: asyncio.Queue[ProcessRecord] = asyncio.Queue(maxsize=settings.queue_size)
        self._latest: dict[str, SignalValue] = {}
        self._node_by_id = {node.node_id: node for node in nodes}
        self._missing_nodes: set[str] = set()
        self._connection_lost = asyncio.Event()
        self._history: dict[str, deque[SignalValue]] = {}
        self._pending: deque[_PendingCycle] = deque()
        self._last_cycle: tuple[str, datetime | None] | None = None
        self._last_snapshot_timestamp: datetime | None = None
        self._fatal_error: str | None = None
        self.connected = False
        self.healthy_at: float | None = None
        self.connection_epoch = 0
        self._running_value = False
        self._running_read_at: float | None = None
        self.diagnostics: deque[tuple[str, str]] = deque(maxlen=1000)

    @property
    def production_running(self) -> bool:
        return bool(
            self.connected
            and self._running_value
            and self._running_read_at is not None
            and 0 <= time.monotonic() - self._running_read_at <= 4
        )

    @property
    def missing_nodes(self) -> tuple[str, ...]:
        return tuple(sorted(self._missing_nodes))

    @classmethod
    def from_configuration(cls, configuration: Any) -> OpcUaProvider:
        raw = configuration.raw.get("opcua", {})
        settings = OpcUaConnectionSettings(
            endpoint=str(raw["endpoint"]),
            security_policy=_string_or_none(raw.get("security_policy")),
            security_mode=_string_or_none(raw.get("security_mode")),
            username=_string_or_none(raw.get("username")),
            password=os.environ.get(str(raw["password_env"])) if raw.get("password_env") else None,
            client_certificate=_string_or_none(raw.get("client_certificate")),
            private_key=_string_or_none(raw.get("private_key")),
            trusted_server_certificate=_string_or_none(raw.get("trusted_server_certificate")),
            subscription_interval_ms=int(raw.get("subscription_interval_ms", 1_000)),
            reconnect_initial_seconds=float(raw.get("reconnect_initial_seconds", 1.0)),
            reconnect_max_seconds=float(raw.get("reconnect_max_seconds", 30.0)),
            queue_size=int(raw.get("queue_size", 1_000)),
            monitored_item_queue_size=int(raw.get("monitored_item_queue_size", 1_000)),
            snapshot_wait_seconds=float(raw.get("snapshot_wait_seconds", 2.0)),
            snapshot_timestamp_tolerance_seconds=float(
                raw.get("snapshot_timestamp_tolerance_seconds", 0.0)
            ),
            asynchronous_max_age_seconds=float(raw.get("asynchronous_max_age_seconds", 60.0)),
            run_id=_string_or_none(raw.get("run_id")),
        )
        if settings.username and not settings.password:
            raise RuntimeError(
                f"OPC UA password environment variable is not available for {settings.username!r}"
            )
        nodes = tuple(
            OpcUaNode(
                signal_id=signal_id,
                node_id=str(metadata["node_id"]),
                display_name=str(metadata.get("display_name", signal_id)),
                unit=_string_or_none(metadata.get("unit")),
                datatype=_string_or_none(metadata.get("expected_datatype")),
                is_cycle_id=bool(metadata.get("is_cycle_id", False)),
                required=bool(metadata.get("required", False)),
                cycle_synchronous=bool(metadata.get("cycle_synchronous", True)),
                is_running_state=bool(metadata.get("is_running_state", False)),
                production_field=_string_or_none(metadata.get("production_field")),
            )
            for signal_id, metadata in configuration.signal_registry.items()
            if metadata.get("node_id")
        )
        return cls(settings, nodes, str(configuration.machine_name))

    async def stop(self) -> None:
        self._stop_event.set()
        self._flush_pending(force=True)

    async def browse_root(self) -> list[str]:
        """Browse root children only; this provider never writes or invokes methods."""
        client = await self._connect_client()
        try:
            return [str(child) for child in await client.nodes.root.get_children()]
        finally:
            await client.disconnect()

    async def records(self) -> AsyncIterator[ProcessRecord]:
        """Yield cycle-triggered immutable snapshots with bounded reconnect backoff."""
        if not any(node.is_cycle_id for node in self.nodes):
            raise RuntimeError("OPC UA registry requires one signal with is_cycle_id: true")
        delay = self.settings.reconnect_initial_seconds
        while not self._stop_event.is_set():
            try:
                client = await self._connect_client()
                try:
                    self._latest.clear()
                    self._running_value = False
                    self._running_read_at = None
                    self._history.clear()
                    self._missing_nodes.clear()
                    self._connection_lost.clear()
                    subscription = await client.create_subscription(
                        self.settings.subscription_interval_ms, _SubscriptionHandler(self)
                    )
                    await self._subscribe(client, subscription)
                    delay = self.settings.reconnect_initial_seconds
                    self.connected = True
                    self.connection_epoch += 1
                    self.healthy_at = time.monotonic()
                    self.diagnostics.append(
                        (
                            "Info",
                            "OPC UA connected; counter gaps require machine-history/export reconciliation",
                        )
                    )
                    while not self._stop_event.is_set() and not self._connection_lost.is_set():
                        if self._fatal_error:
                            raise RuntimeError(self._fatal_error)
                        self._flush_pending()
                        if self.healthy_at is None or time.monotonic() - self.healthy_at >= 1:
                            await asyncio.wait_for(client.nodes.server_state.read_value(), 3.0)
                            for spec in self.nodes:
                                if spec.is_running_state:
                                    current = await asyncio.wait_for(
                                        client.get_node(spec.node_id).read_data_value(), 3.0
                                    )
                                    self._running_value = (
                                        _quality(current.StatusCode) is QualityStatus.GOOD
                                        and current.Value.Value is True
                                    )
                                    self._running_read_at = time.monotonic()
                            self.healthy_at = time.monotonic()
                        try:
                            yield await asyncio.wait_for(self._records.get(), timeout=0.25)
                        except TimeoutError:
                            continue
                finally:
                    self.connected = False
                    self._flush_pending(force=True)
                    await client.disconnect()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                LOG.warning("OPC UA acquisition connection failed: %s", error)
                self.connected = False
                self.diagnostics.append(("Error", f"OPC UA disconnected: {error}"))
                if self._fatal_error:
                    raise RuntimeError(self._fatal_error) from error
            if not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(self._stop_event.wait(), delay)
                except TimeoutError:
                    pass
                delay = min(self.settings.reconnect_max_seconds, max(delay * 2, 0.1))
        self._flush_pending(force=True)
        while not self._records.empty():
            yield self._records.get_nowait()

    async def _connect_client(self) -> Any:
        try:
            from asyncua.client.client import Client
        except ImportError as error:
            raise RuntimeError("Install edge-logger[opcua] to enable OPC UA acquisition") from error
        client = Client(url=self.settings.endpoint)
        if self.settings.username:
            client.set_user(self.settings.username)
        if self.settings.password:
            client.set_password(self.settings.password)
        if self.settings.security_mode not in {None, "None"}:
            if not all(
                (
                    self.settings.security_policy,
                    self.settings.client_certificate,
                    self.settings.private_key,
                    self.settings.trusted_server_certificate,
                )
            ):
                raise RuntimeError(
                    "Secure OPC UA mode requires policy, client certificate, private key, and server certificate"
                )
            security = ",".join(
                (
                    str(self.settings.security_policy),
                    str(self.settings.security_mode),
                    str(self.settings.client_certificate),
                    str(self.settings.private_key),
                    str(self.settings.trusted_server_certificate),
                )
            )
            await client.set_security_string(security)
        await client.connect()
        return client

    async def _subscribe(self, client: Any, subscription: Any) -> None:
        from asyncua import ua
        from asyncua.ua.attribute_ids import AttributeIds

        monitored: list[Any] = []
        for spec in self.nodes:
            node = client.get_node(spec.node_id)
            try:
                await node.read_data_value()
            except Exception as error:
                self._missing_nodes.add(spec.node_id)
                LOG.error("Configured OPC UA node %s is unavailable: %s", spec.node_id, error)
                if spec.required or spec.is_cycle_id:
                    raise RuntimeError(
                        f"Required OPC UA node is unavailable: {spec.node_id}"
                    ) from error
                continue
            monitored.append(node)
        if not monitored:
            raise RuntimeError("No configured OPC UA nodes were available for subscription")
        requests = []
        for handle, node in enumerate(monitored, start=1):
            request = ua.MonitoredItemCreateRequest()
            request.ItemToMonitor.NodeId = node.nodeid
            request.ItemToMonitor.AttributeId = AttributeIds.Value
            request.MonitoringMode = ua.MonitoringMode.Reporting
            request.RequestedParameters.ClientHandle = handle
            request.RequestedParameters.SamplingInterval = float(
                self.settings.subscription_interval_ms
            )
            request.RequestedParameters.QueueSize = self.settings.monitored_item_queue_size
            request.RequestedParameters.DiscardOldest = True
            request.RequestedParameters.Filter = ua.DataChangeFilter(
                Trigger=ua.DataChangeTrigger.StatusValueTimestamp
            )
            requests.append(request)
        results = await subscription.create_monitored_items(requests)
        if len(results) != len(requests) or any(not isinstance(item, int) for item in results):
            raise RuntimeError(f"OPC UA monitored items were rejected: {results}")

    async def _data_change(self, node: Any, value: object, data: Any) -> None:
        spec = self._node_by_id.get(node.nodeid.to_string())
        if spec is None:
            LOG.warning("Received data change for unconfigured node %s", node.nodeid)
            return
        received = datetime.now(UTC)
        data_value = getattr(getattr(data, "monitored_item", None), "Value", None)
        source_timestamp = getattr(data_value, "SourceTimestamp", None)
        server_timestamp = getattr(data_value, "ServerTimestamp", None)
        status = getattr(data_value, "StatusCode", None)
        signal = SignalValue(
            signal_id=spec.signal_id,
            display_name=spec.display_name,
            value=value,
            unit=spec.unit,
            datatype=spec.datatype,
            quality=_quality(status),
            source_timestamp=source_timestamp,
            server_timestamp=server_timestamp,
            status_code=f"{getattr(status, 'name', 'Unknown')} (0x{getattr(status, 'value', 0):08x})"
            if hasattr(status, "name")
            else str(status)
            if status is not None
            else None,
        )
        expected = (spec.datatype or "").lower()
        valid_type = (
            isinstance(value, (int, float)) and not isinstance(value, bool)
            if expected in {"float", "numeric", "number"}
            else isinstance(value, int) and not isinstance(value, bool)
            if expected == "int"
            else isinstance(value, bool)
            if expected in {"bool", "boolean"}
            else isinstance(value, str)
            if expected in {"str", "string"}
            else True
        )
        if value is None and signal.quality is QualityStatus.GOOD:
            signal = replace(signal, quality=QualityStatus.MISSING)
        elif not valid_type and signal.quality is QualityStatus.GOOD:
            signal = replace(signal, quality=QualityStatus.PARSE_ERROR, raw_value=str(value))
        if isinstance(value, float) and not math.isfinite(value):
            signal = replace(
                signal, quality=QualityStatus.INVALID_NUMERIC, raw_value=str(value), value=None
            )
        numeric_status = getattr(status, "value", 0)
        if isinstance(numeric_status, int) and numeric_status & 0x80:
            self.diagnostics.append(
                (
                    "Error",
                    f"OPC UA queue overflow for {spec.signal_id}; machine-history/export reconciliation required",
                )
            )
            signal = replace(signal, quality=QualityStatus.UNCERTAIN)
        self._latest[spec.signal_id] = signal
        if spec.is_running_state:
            self._running_value = signal.quality is QualityStatus.GOOD and signal.value is True
            self._running_read_at = time.monotonic()
        self._history.setdefault(spec.signal_id, deque(maxlen=self.settings.queue_size)).append(
            signal
        )
        if spec.is_cycle_id:
            identity = (str(value), source_timestamp)
            if self._last_cycle is not None and identity[0] == self._last_cycle[0]:
                return
            if (
                signal.quality is not QualityStatus.GOOD
                or source_timestamp is None
                or value is None
            ):
                self.diagnostics.append(
                    (
                        "Error",
                        "Cycle trigger has invalid quality or no source timestamp; record not accepted",
                    )
                )
                return
            if len(self._pending) >= self.settings.queue_size:
                self._fatal_error = "Cycle snapshot buffer overflow; acquisition stopped rather than silently dropping cycles"
                return
            self._last_cycle = identity
            self._pending.append(
                _PendingCycle(
                    signal, received, time.monotonic() + self.settings.snapshot_wait_seconds
                )
            )
        self._flush_pending()

    def _flush_pending(self, force: bool = False) -> None:
        while self._pending:
            pending = self._pending[0]
            timestamp = pending.signal.source_timestamp
            assert timestamp is not None
            values: list[SignalValue] = []
            complete = True
            for spec in self.nodes:
                if spec.is_cycle_id:
                    values.append(pending.signal)
                    continue
                candidates = self._history.get(spec.signal_id, ())
                fresh = [
                    value
                    for value in candidates
                    if value.source_timestamp is not None
                    and (
                        not spec.cycle_synchronous
                        or self._last_snapshot_timestamp is None
                        or value.source_timestamp > self._last_snapshot_timestamp
                    )
                    and (
                        abs((value.source_timestamp - timestamp).total_seconds())
                        <= self.settings.snapshot_timestamp_tolerance_seconds
                        if spec.cycle_synchronous
                        else 0
                        <= (timestamp - value.source_timestamp).total_seconds()
                        <= self.settings.asynchronous_max_age_seconds
                    )
                ]
                if fresh:
                    values.append(
                        min(
                            reversed(fresh),
                            key=lambda value: abs(
                                ((value.source_timestamp or timestamp) - timestamp).total_seconds()
                            ),
                        )
                    )
                else:
                    complete = False
                    values.append(
                        SignalValue(
                            spec.signal_id,
                            spec.display_name,
                            None,
                            spec.unit,
                            spec.datatype,
                            QualityStatus.MISSING,
                            timestamp,
                        )
                    )
            if not complete and not force and time.monotonic() < pending.expires:
                return
            context = {
                spec.production_field: signal.value
                for spec in self.nodes
                if spec.production_field
                for signal in values
                if signal.signal_id == spec.signal_id and signal.quality is QualityStatus.GOOD
            }
            context["snapshot_complete"] = complete
            cycle_spec = next(spec for spec in self.nodes if spec.is_cycle_id)
            record = ProcessRecord(
                self.machine_name,
                timestamp,
                pending.received,
                str(pending.signal.value),
                f"opcua:{cycle_spec.node_id}",
                tuple(values),
                str(context["run_id"])
                if context.get("run_id") is not None
                else self.settings.run_id,
                context,
            )
            try:
                self._records.put_nowait(record)
            except asyncio.QueueFull:
                self._fatal_error = (
                    "Process record queue overflow; acquisition stopped; reconcile machine history"
                )
                return
            if not complete:
                self.diagnostics.append(
                    (
                        "Error",
                        f"Incomplete snapshot for cycle {record.cycle_id}; missing readings retained as Missing",
                    )
                )
            self._pending.popleft()
            self._last_snapshot_timestamp = timestamp

    async def _status_change(self, status: object) -> None:
        LOG.warning("OPC UA subscription status changed: %s", status)
        self._connection_lost.set()


class _SubscriptionHandler:
    """asyncua callback adapter; it only forwards read-only notifications."""

    def __init__(self, provider: OpcUaProvider) -> None:
        self.provider = provider

    async def datachange_notification(self, node: Any, value: object, data: Any) -> None:
        await self.provider._data_change(node, value, data)

    async def status_change_notification(self, status: object) -> None:
        await self.provider._status_change(status)


def _quality(status: object | None) -> QualityStatus:
    if status is None:
        return QualityStatus.MISSING
    if hasattr(status, "is_bad") and status.is_bad():
        return QualityStatus.BAD
    if hasattr(status, "is_uncertain") and status.is_uncertain():
        return QualityStatus.UNCERTAIN
    if hasattr(status, "is_good"):
        return QualityStatus.GOOD if status.is_good() else QualityStatus.BAD
    status_text = str(status).lower()
    if "bad" in status_text:
        return QualityStatus.BAD
    if "uncertain" in status_text:
        return QualityStatus.UNCERTAIN
    return QualityStatus.GOOD if "good" in status_text else QualityStatus.MISSING


def _string_or_none(value: object) -> str | None:
    return str(value) if value is not None else None
