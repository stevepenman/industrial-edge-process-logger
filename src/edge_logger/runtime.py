"""Long-running coordinator for acquisition, report sealing, and recovery."""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import sqlite3
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

from . import __version__
from .acquisition.base import AcquisitionProvider
from .acquisition.file_export import FileExportImporter, SourceInspection
from .config import AppConfig
from .email.outbox import EmailAttemptResult, EmailOutboxProcessor
from .health import collect_health
from .metrics import PipelineMetrics
from .models import ProcessRecord, ReportPeriodState
from .reporting.generator import ReportGenerationError, generate_report_from_archives
from .retention.executor import RetentionResult, run_retention
from .storage.audit_db import AuditDatabase
from .storage.exporter import export_records
from .storage.raw_archive import RawArchiveWriter, archive_checksum
from .storage.recovery import recover_archives
from .storage.source_archive import archive_source_file
from .ups.base import UpsProvider, UpsStatus
from .ups.monitor import UpsShutdownController

LOG = logging.getLogger(__name__)


def _join_source_details(details: list[SourceInspection], attribute: str) -> str | None:
    values = [str(getattr(detail, attribute)) for detail in details]
    return "; ".join(values) if values else None


def _join_headers(details: list[SourceInspection]) -> str | None:
    values = [" | ".join(detail.columns) for detail in details]
    return "\n\n".join(values) if values else None


@dataclass(frozen=True, slots=True)
class ImportResult:
    inspection: SourceInspection
    checksum: str
    duplicate: bool
    imported_records: int
    sealed_report_ids: tuple[str, ...]
    metrics: dict[str, object]


class IdleProvider(AcquisitionProvider):
    """Keeps the service alive safely until an OEM-configured provider is enabled."""

    def __init__(self, stop_event: asyncio.Event) -> None:
        self.stop_event = stop_event

    async def records(self):  # type: ignore[no-untyped-def]
        await self.stop_event.wait()
        if False:
            yield ProcessRecord  # pragma: no cover


class LoggerRuntime:
    """Coordinates durable components; analysis and reporting remain separate modules."""

    def __init__(
        self,
        configuration: AppConfig,
        database: AuditDatabase,
        ups_provider: UpsProvider | None = None,
        shutdown_callback: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.configuration = configuration
        self.database = database
        self.ups_provider = ups_provider
        self.shutdown_callback = shutdown_callback
        self.stop_event = asyncio.Event()
        self.shutdown_pending = False
        self._shutdown_requested = False
        self._provider: AcquisitionProvider | None = None
        self._active_writer: RawArchiveWriter | None = None
        self._active_period_id: str | None = None
        self._active_segment_id: str | None = None
        self._active_source_checksum: str | None = None
        self._last_acquired_monotonic: float | None = None
        self._last_acquired_timestamp: datetime | None = None
        self._last_received_timestamp: datetime | None = None
        self._last_cycle: str | None = None
        self._last_observation_checkpoint = float("-inf")
        self._building: set[str] = set()
        self._report_lock = asyncio.Lock()
        self._last_ups_status: UpsStatus | None = None
        self._report_ready = asyncio.Event()
        self._background_error: BaseException | None = None
        self._started_at = datetime.now(UTC)
        self._last_archive_checkpoint = time.monotonic()
        self._ups_controller = UpsShutdownController(
            float(configuration.raw.get("ups", {}).get("shutdown_delay_seconds", 90))
        )
        self.outbox = EmailOutboxProcessor(dict(configuration.raw.get("email", {})), database)

    @property
    def threshold_seconds(self) -> float:
        return self.configuration.reporting.acquisition_hours * 3600

    async def recover(self, build_reports: bool = True) -> None:
        """Reconcile durable cycle records, archive mirrors and interrupted work."""
        recover_archives(self.database, self.configuration.raw_path)
        interrupted = self.database.reset_interrupted_report_builds()
        for period in interrupted:
            temporary = self.configuration.reports_path / f"{period['report_id']}.tmp.xlsx"
            if temporary.exists():
                temporary.unlink()
        current = self.database.open_period()
        due = (
            self.database.seal_due_period(self.threshold_seconds)
            if current is None
            or current["record_count"] >= self.configuration.reporting.minimum_records
            else None
        )
        if due is not None:
            LOG.info("Recovered due report period %s", due["report_id"])
        ambiguous_sends = self.database.recover_interrupted_email_sends()
        if ambiguous_sends:
            LOG.warning("Marked %s interrupted SMTP attempt(s) ambiguous", ambiguous_sends)
        if build_reports and self.configuration.reporting.generate_if_due_on_startup:
            await self.process_pending_reports()
        for period in self.database.periods():
            if period["state"] == ReportPeriodState.REPORT_VALIDATED.value:
                self.outbox.queue(str(period["id"]))
        if build_reports and not self.shutdown_pending:
            await self.outbox.process_pending_async()

    async def import_file(
        self, source: Path, dry_run: bool = False, force_report: bool = False
    ) -> ImportResult:
        importer = FileExportImporter(
            self.configuration.machine_name,
            self.configuration.signal_registry,
            self.configuration.timezone,
        )
        inspection = importer.inspect(source)
        checksum = importer.checksum(source)
        if dry_run:
            return ImportResult(inspection, checksum, False, 0, (), {})
        metrics = PipelineMetrics()
        with metrics.phase("source_archive"):
            archive = archive_source_file(source, self.configuration.raw_path / "source")
        checksum = archive.sha256
        inspection = importer.inspect(archive.archive_path)
        if self._active_writer is None:
            recover_archives(self.database, self.configuration.raw_path)
        initial_period = self.database.get_or_create_open_period()
        if self.database.import_complete(checksum):
            return ImportResult(inspection, checksum, True, 0, (), metrics.as_dict())
        self.database.register_source_file(
            checksum,
            archive.original_name,
            archive.size_bytes,
            archive.archive_path,
            initial_period["id"],
            import_state="IN_PROGRESS",
        )
        sealed: list[str] = []
        imported = 0
        self._active_source_checksum = checksum
        try:
            iterator = importer.records(archive.archive_path)
            while True:
                try:
                    with metrics.phase("import_parser"):
                        record = next(iterator)
                except StopIteration:
                    break
                # A historical OEM export has no evidence of Raspberry Pi healthy-acquisition
                # time. Its timestamp gaps can represent downtime or paused export recording,
                # so a replay/import never fabricates report-trigger hours from source time.
                if self.database.has_record(record, checksum):
                    continue
                period = await self.ingest_record(record, 0.0, metrics)
                imported += 1
                if period is not None:
                    sealed.append(str(period["report_id"]))
            await self._close_active_segment()
            self.database.complete_import(checksum)
            if force_report:
                forced = self.database.force_seal_open_period()
                if forced is not None:
                    sealed.append(str(forced["report_id"]))
            await self.process_pending_reports()
        finally:
            await self._close_active_segment()
            self._active_source_checksum = None
        return ImportResult(inspection, checksum, False, imported, tuple(sealed), metrics.as_dict())

    async def ingest_record(
        self,
        record: ProcessRecord,
        acquisition_seconds: float,
        metrics: PipelineMetrics | None = None,
    ) -> sqlite3.Row | None:
        context = dict(self.configuration.raw.get("production", {}))
        if context:
            record = replace(record, production_context={**context, **record.production_context})
        if self.database.has_record(record, self._active_source_checksum):
            return None
        if record.source_identifier.startswith("opcua:"):
            previous = self.database.connection.execute(
                "SELECT cycle_id, run_id, payload_json FROM process_records WHERE machine_name = ? ORDER BY sequence DESC LIMIT 1",
                (record.machine_name,),
            ).fetchone()
            if (
                previous is not None
                and previous["run_id"] == record.run_id
                and json.loads(previous["payload_json"])["source_identifier"].startswith("opcua:")
            ):
                if previous["cycle_id"] == record.cycle_id:
                    return None  # A refreshed timestamp or restart snapshot is not a new shot.
                try:
                    delta = int(str(record.cycle_id)) - int(str(previous["cycle_id"]))
                except ValueError:
                    delta = 1
                if delta != 1:
                    self.database.record_system_event(
                        "Acquisition",
                        "Error",
                        f"Machine cycle counter changed from {previous['cycle_id']} to {record.cycle_id}; reconcile OEM history/export and run identity",
                    )
        period = self.database.get_or_create_open_period()
        await self._ensure_segment(str(period["id"]))
        assert self._active_writer is not None
        assert self._active_segment_id is not None
        if not self.database.persist_record(
            record,
            str(period["id"]),
            self._active_segment_id,
            acquisition_seconds,
            self._active_source_checksum,
        ):
            return None
        if metrics is None:
            self._active_writer.append(record)
        else:
            with metrics.phase("raw_archive"):
                self._active_writer.append(record)
        self._last_acquired_timestamp = record.source_timestamp
        self._last_received_timestamp = record.received_timestamp
        self._last_cycle = record.cycle_id
        self._checkpoint_observation()
        updated = self.database.period(str(period["id"]))
        size_due = updated["record_count"] >= self.configuration.reporting.maximum_records
        if updated["accumulated_seconds"] < self.threshold_seconds and not size_due:
            return None
        await self._close_active_segment()
        sealed = self.database.seal_due_period(0 if size_due else self.threshold_seconds)
        if sealed is not None:
            LOG.info("Sealed report period %s", sealed["report_id"])
        return sealed

    async def run(self, provider: AcquisitionProvider | None = None) -> None:
        await self.recover(build_reports=False)
        selected_provider = provider or IdleProvider(self.stop_event)
        self._provider = selected_provider
        tasks = [
            asyncio.create_task(self._clock_loop()),
            asyncio.create_task(self._report_loop()),
            asyncio.create_task(self._email_loop()),
            asyncio.create_task(self._operations_loop()),
        ]
        if self.ups_provider:
            tasks.append(asyncio.create_task(self._monitor_ups()))
        for task in tasks:
            task.add_done_callback(self._worker_done)
        try:
            async for record in selected_provider.records():
                sealed = await self.ingest_record(record, 0.0)
                if sealed is not None and not self.shutdown_pending:
                    self._report_ready.set()
        finally:
            self.stop_event.set()
            self._provider = None
            await self._close_active_segment()
            self.database.set_application_state(
                "last_clean_shutdown", datetime.now(UTC).isoformat()
            )
            self.database.set_application_state(
                "runtime_observation",
                json.dumps(
                    {
                        "observed_at": datetime.now(UTC).isoformat(),
                        "acquisition_connected": False,
                        "stopped": True,
                    }
                ),
            )
            if (
                self.shutdown_pending
                and self.shutdown_callback is not None
                and not self._shutdown_requested
            ):
                await self.shutdown_callback()
                self._shutdown_requested = True
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if not self.shutdown_pending and self._background_error is None:
                await self.process_pending_reports()
                await self.outbox.process_pending_async()
            if (
                self.shutdown_pending
                and self.shutdown_callback is not None
                and not self._shutdown_requested
            ):
                await self.shutdown_callback()
        if self._background_error is not None:
            raise RuntimeError("A background service failed") from self._background_error

    def _worker_done(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            self._background_error = error
            asyncio.create_task(self.request_stop())

    async def _pause(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop_event.wait(), seconds)
        except TimeoutError:
            pass

    def _healthy(self) -> bool:
        provider = self._provider
        heartbeat = getattr(provider, "healthy_at", None)
        configured_gap = self.configuration.raw.get("acquisition", {}).get(
            "communication_gap_seconds"
        )
        maximum_age = float(configured_gap) if configured_gap is not None else 4.0
        return bool(
            getattr(provider, "connected", False)
            and heartbeat is not None
            and 0 <= time.monotonic() - heartbeat <= maximum_age
        )

    async def _clock_loop(self) -> None:
        previous = time.monotonic()
        previously_healthy = False
        previously_running = False
        epoch = getattr(self._provider, "connection_epoch", None)
        while not self.stop_event.is_set():
            now = time.monotonic()
            healthy = self._healthy()
            running = bool(getattr(self._provider, "production_running", False))
            current_epoch = getattr(self._provider, "connection_epoch", None)
            elapsed = now - previous
            if (
                healthy
                and previously_healthy
                and current_epoch == epoch
                and 0 <= elapsed <= 3
                and (
                    self.configuration.reporting.trigger_mode == "acquisition_hours"
                    or (running and previously_running)
                )
            ):
                self.database.add_acquisition_seconds(elapsed)
            previous, previously_healthy, previously_running, epoch = (
                now,
                healthy,
                running,
                current_epoch,
            )
            await self._seal_if_due()
            if self._active_writer is not None and now - self._last_archive_checkpoint >= 30:
                self._active_writer.checkpoint()
                self._last_archive_checkpoint = now
            await self._pause(1)

    async def _seal_if_due(self) -> None:
        period = self.database.open_period()
        if period is None or period["record_count"] < self.configuration.reporting.minimum_records:
            return
        days = self.configuration.reporting.maximum_calendar_days
        calendar_due = (
            days is not None
            and (datetime.now(UTC) - datetime.fromisoformat(period["started_at"])).total_seconds()
            >= days * 86400
        )
        size_due = period["record_count"] >= self.configuration.reporting.maximum_records
        if period["accumulated_seconds"] >= self.threshold_seconds or calendar_due or size_due:
            await self._close_active_segment()
            self.database.seal_due_period(0 if calendar_due or size_due else self.threshold_seconds)
            self._report_ready.set()

    async def _report_loop(self) -> None:
        # Startup generation can be explicitly deferred until the next newly sealed period.
        if not self.configuration.reporting.generate_if_due_on_startup:
            await self._report_ready.wait()
        while not self.stop_event.is_set():
            self._report_ready.clear()
            await self.process_pending_reports()
            try:
                await asyncio.wait_for(self._report_ready.wait(), 5)
            except TimeoutError:
                pass

    async def _email_loop(self) -> None:
        while not self.stop_event.is_set():
            for period in self.database.periods():
                if period["state"] == ReportPeriodState.REPORT_VALIDATED.value:
                    self.outbox.queue(str(period["id"]))
            try:
                await self.outbox.process_pending_async()
            except (OSError, ValueError) as error:
                self.database.record_system_event("Email", "Error", str(error))
            await self._pause(5)

    async def _operations_loop(self) -> None:
        while not self.stop_event.is_set():
            provider = self._provider
            diagnostics = getattr(provider, "diagnostics", None)
            if diagnostics is not None:
                while diagnostics:
                    severity, detail = diagnostics.popleft()
                    self.database.record_system_event("Acquisition", severity, detail)
            health = await asyncio.to_thread(collect_health, self.configuration.raw_path)
            healthy = self._healthy()
            observation = {
                "observed_at": datetime.now(UTC).isoformat(),
                "acquisition_connected": healthy,
                "health": health.as_dict(),
                "last_cycle": self._last_cycle,
                "last_received_timestamp": self._last_received_timestamp.isoformat()
                if self._last_received_timestamp
                else None,
                "ups": None
                if self._last_ups_status is None
                else {
                    "external_power_present": self._last_ups_status.external_power_present,
                    "battery_critical": self._last_ups_status.battery_critical,
                    "battery_percent": self._last_ups_status.battery_percent,
                },
            }
            self.database.set_application_state("runtime_observation", json.dumps(observation))
            operations = self.configuration.raw.get("operations", {})
            low_disk = health.disk_free_bytes < int(
                operations.get("minimum_free_disk_bytes", 1024**3)
            )
            stale_seconds = float(operations.get("stale_data_seconds", 300))
            stale = (
                bool(getattr(provider, "nodes", None))
                and (
                    datetime.now(UTC) - (self._last_received_timestamp or self._started_at)
                ).total_seconds()
                > stale_seconds
            )
            if (
                low_disk
                or stale
                or (not healthy and getattr(provider, "nodes", None))
                or health.ntp_synchronized is False
            ):
                self.database.record_system_event(
                    "Health",
                    "Warning",
                    f"low_disk={low_disk}; stale_data={stale}; acquisition_connected={healthy}; ntp={health.ntp_synchronized}",
                )
            if operations.get("automatic_retention", True):
                await asyncio.to_thread(self._retention_worker)
            await self._pause(float(operations.get("poll_seconds", 30)))

    def _retention_worker(self) -> None:
        database = AuditDatabase(self.database.path)
        try:
            run_retention(
                database,
                self.configuration.reports_path,
                self.configuration.raw_path,
                int(
                    self.configuration.raw.get("retention", {}).get(
                        "days_after_successful_email", 30
                    )
                ),
                datetime.now(UTC),
                False,
            )
        finally:
            database.close()

    async def request_stop(self) -> None:
        self.stop_event.set()
        stopper = getattr(self._provider, "stop", None)
        if stopper is not None:
            await stopper()

    async def process_pending_reports(self) -> None:
        if self.shutdown_pending:
            return
        async with self._report_lock:
            while not self.shutdown_pending:
                pending = self.database.pending_periods()
                due = next(
                    (
                        period
                        for period in pending
                        if period["state"] == ReportPeriodState.DUE.value
                    ),
                    None,
                )
                if due is not None:
                    self.database.seal_due_period(self.threshold_seconds)
                    continue
                sealed = next(
                    (
                        period
                        for period in pending
                        if period["state"] == ReportPeriodState.SEALED.value
                    ),
                    None,
                )
                if sealed is None:
                    return
                period_id = str(sealed["id"])
                if period_id in self._building:
                    return
                self._building.add(period_id)
                try:
                    await self._build_report(period_id)
                finally:
                    self._building.discard(period_id)

    async def _build_report(self, period_id: str) -> None:
        metrics = PipelineMetrics()
        period = self.database.period(period_id)
        if period["state"] != ReportPeriodState.SEALED.value:
            return
        if period["record_count"] < self.configuration.reporting.minimum_records:
            LOG.warning("Not generating empty report period %s", period["report_id"])
            self.database.transition_period(period_id, ReportPeriodState.ERROR)
            return
        with metrics.phase("sqlite_operations"):
            self.database.transition_period(period_id, ReportPeriodState.REPORT_BUILDING)
            archives = self.database.raw_segments_for_period(period_id)
        paths = [Path(str(row["path"])) for row in archives]
        if sum(int(row["record_count"]) for row in archives) != period["record_count"]:
            self.database.record_system_event(
                "Reporting", "Error", "Archive counts do not match committed cycle count", period_id
            )
            self.database.transition_period(period_id, ReportPeriodState.ERROR)
            return
        for row, path in zip(archives, paths, strict=True):
            if (
                not row["closed_at"]
                or await asyncio.to_thread(archive_checksum, path) != row["sha256"]
            ):
                raise RuntimeError(f"Unclosed or damaged raw archive: {path}")
        sources = self.database.source_files_for_period(period_id)
        if any(source["import_state"] != "COMPLETE" for source in sources):
            self.database.record_system_event(
                "Reporting",
                "Error",
                "Source import is incomplete; resume import before retry-report",
                period_id,
            )
            self.database.transition_period(period_id, ReportPeriodState.ERROR)
            return
        source_details: list[SourceInspection] = []
        inspector = FileExportImporter(
            self.configuration.machine_name,
            self.configuration.signal_registry,
            self.configuration.timezone,
        )
        for source in sources:
            archive_path = source["archive_path"]
            if archive_path is None:
                continue
            try:
                source_details.append(
                    await asyncio.to_thread(inspector.inspect, Path(str(archive_path)))
                )
            except (OSError, ValueError):
                LOG.warning(
                    "Unable to re-inspect archived source %s for manifest diagnostics", archive_path
                )
        historical_import = bool(source_details)
        manifest = {
            "acquisition_hours": "N/A – historical import"
            if historical_import
            else float(period["accumulated_seconds"]) / 3600,
            "original_filenames": "; ".join(str(row["original_name"]) for row in sources),
            "original_sizes": "; ".join(str(row["size_bytes"]) for row in sources),
            "source_checksums": "; ".join(str(row["sha256"]) for row in sources),
            "raw_archive_ids": "; ".join(str(row["id"]) for row in archives),
            "raw_archive_checksums": "; ".join(str(row["sha256"]) for row in archives),
            "hostname": socket.gethostname(),
            "application_version": __version__,
            "configuration_version": self.configuration.config_version,
            "acquisition_provider": "File import"
            if historical_import
            else self.configuration.raw.get("acquisition", {}).get("provider", "unknown"),
            "source_encoding": _join_source_details(source_details, "encoding"),
            "source_delimiter": _join_source_details(source_details, "delimiter"),
            "physical_source_column_count": _join_source_details(
                source_details, "physical_column_count"
            ),
            "named_source_field_count": _join_source_details(source_details, "named_column_count"),
            "ignored_blank_trailing_column_count": _join_source_details(
                source_details, "ignored_blank_trailing_columns"
            ),
            "source_ordering": _join_source_details(source_details, "source_ordering"),
            "source_headers": _join_headers(source_details),
        }
        try:
            worker = asyncio.create_task(
                asyncio.to_thread(
                    generate_report_from_archives,
                    paths,
                    self.configuration.reports_path,
                    str(period["report_id"]),
                    self.configuration.machine_name,
                    self.configuration.timezone,
                    manifest,
                    self.configuration.signal_registry,
                    self.configuration.rules,
                    self.configuration.reporting.trend_bucket_minutes,
                    metrics,
                    self.configuration.reporting.trend_bucket_count,
                )
            )
            try:
                path, checksum = await asyncio.shield(worker)
            except asyncio.CancelledError:
                # A worker owns its temporary file until completion; never race a rebuild.
                path, checksum = await worker
                if self.shutdown_pending:
                    return
        except (OSError, ReportGenerationError, ValueError):
            LOG.exception("Report generation failed for %s", period["report_id"])
            self.database.transition_period(period_id, ReportPeriodState.ERROR)
            return
        metadata = await asyncio.to_thread(
            export_records,
            self.database.path,
            self.configuration.reports_path,
            period_id,
            str(period["report_id"]),
            int(period["record_count"]),
        )
        self.database.set_application_state(f"process_export:{period_id}", json.dumps(metadata))
        with metrics.phase("sqlite_operations"):
            self.database.set_workbook(period_id, path, checksum)
            self.database.transition_period(period_id, ReportPeriodState.REPORT_VALIDATED)
            self.database.set_application_state(
                f"report_metrics:{period_id}", json.dumps(metrics.as_dict(), sort_keys=True)
            )
            self.database.set_application_state(
                "last_report_metrics", json.dumps(metrics.as_dict(), sort_keys=True)
            )
        self.outbox.queue(period_id)

    def export_period(self, report_id: str) -> dict[str, object]:
        period = self.database.report_period(report_id)
        if period["state"] == ReportPeriodState.OPEN.value:
            raise ValueError("Only sealed periods can be exported")
        metadata = export_records(
            self.database.path,
            self.configuration.reports_path,
            str(period["id"]),
            report_id,
            int(period["record_count"]),
        )
        self.database.set_application_state(f"process_export:{period['id']}", json.dumps(metadata))
        return metadata

    def retry_email(self, report_id: str) -> EmailAttemptResult:
        return self.outbox.retry(report_id)

    async def retry_report(self, report_id: str) -> None:
        period = self.database.report_period(report_id)
        if (
            period["state"] != ReportPeriodState.ERROR.value
            or self.database.email_outbox(str(period["id"])) is not None
        ):
            raise ValueError(
                "retry-report requires a failed build with no email attempt; use retry-email for delivery failures"
            )
        recover_archives(self.database, self.configuration.raw_path)
        with self.database.transaction():
            self.database.connection.execute(
                "UPDATE report_periods SET state = ?, error_detail = NULL WHERE id = ?",
                (ReportPeriodState.SEALED.value, period["id"]),
            )
        self.database.record_system_event(
            "Reporting",
            "Info",
            "Operator requested report rebuild from retained canonical records",
            str(period["id"]),
        )
        await self.process_pending_reports()

    def email_test(self) -> dict[str, str]:
        return {"message_id": self.outbox.send_test(), "state": "accepted"}

    def retention_check(self, dry_run: bool = True) -> tuple[RetentionResult, ...]:
        retention = self.configuration.raw.get("retention", {})
        return run_retention(
            self.database,
            self.configuration.reports_path,
            self.configuration.raw_path,
            int(retention.get("days_after_successful_email", 30)),
            datetime.now(UTC),
            dry_run,
        )

    async def _ensure_segment(self, period_id: str) -> None:
        if self._active_writer is not None and self._active_period_id == period_id:
            return
        await self._close_active_segment()
        identifier = f"{period_id}-{uuid.uuid4()}"
        self._active_writer = RawArchiveWriter(self.configuration.raw_path / "records", identifier)
        self._active_period_id = period_id
        self._active_segment_id = self.database.open_raw_segment(
            period_id, self._active_writer.path
        )
        if self._active_source_checksum:
            self.database.associate_source_with_period(self._active_source_checksum, period_id)

    async def _close_active_segment(self) -> None:
        if self._active_writer is None or self._active_period_id is None:
            return
        checksum = self._active_writer.close()
        count = self.database.connection.execute(
            "SELECT COUNT(*) FROM process_records WHERE raw_segment_id = ?",
            (self._active_segment_id,),
        ).fetchone()[0]
        if count != self._active_writer.record_count:
            from .storage.raw_archive import rebuild_archive

            checksum, count = rebuild_archive(
                self._active_writer.path,
                self.database.segment_records(str(self._active_segment_id)),
            )
        self.database.register_raw_segment(
            self._active_period_id,
            self._active_writer.path,
            checksum,
            count,
        )
        self._active_writer = None
        self._active_period_id = None
        self._active_segment_id = None

    async def _monitor_ups(self) -> None:
        assert self.ups_provider is not None
        while not self.stop_event.is_set():
            status = await self.ups_provider.status()
            self._last_ups_status = status
            if self._ups_controller.observe(status, time.monotonic()):
                reason = (
                    "UPS battery is critical"
                    if status.battery_critical
                    else "UPS external power outage exceeded shutdown delay"
                )
                await self._begin_ups_shutdown(reason)
                return
            await asyncio.sleep(1)

    async def _begin_ups_shutdown(self, reason: str) -> None:
        LOG.warning("Beginning graceful shutdown: %s", reason)
        self.shutdown_pending = True
        self.database.set_application_state("shutdown_reason", reason)
        await self.request_stop()

    def status(self) -> dict[str, object]:
        open_period = self.database.open_period()
        health = collect_health(self.configuration.raw_path)
        periods = self.database.periods()
        last_report = next((row for row in reversed(periods) if row["workbook_path"]), None)
        last_email = next((row for row in reversed(periods) if row["email_sent_at"]), None)
        timestamp = (
            self._last_acquired_timestamp.isoformat()
            if self._last_acquired_timestamp
            else self.database.application_state("last_acquired_timestamp")
        )
        received = (
            self._last_received_timestamp.isoformat()
            if self._last_received_timestamp
            else self.database.application_state("last_received_timestamp")
        )
        cycle = self._last_cycle or self.database.application_state("last_cycle")
        report_counts = self.database.report_counts_by_state()
        email_counts = self.database.email_counts_by_state()
        accumulated_seconds = float(open_period["accumulated_seconds"]) if open_period else 0.0
        records = int(open_period["record_count"]) if open_period else 0
        ups_status = self._last_ups_status
        report_timing = self.database.application_state("last_report_metrics")
        observed = self.database.application_state("runtime_observation")
        connected = self._healthy()
        service_running = self._provider is not None
        if observed and not service_running:
            try:
                snapshot = json.loads(observed)
                age = (
                    datetime.now(UTC) - datetime.fromisoformat(snapshot["observed_at"])
                ).total_seconds()
                service_running = (
                    0
                    <= age
                    <= max(
                        90,
                        float(self.configuration.raw.get("operations", {}).get("poll_seconds", 30))
                        * 3,
                    )
                )
                service_running = service_running and not snapshot.get("stopped", False)
                connected = service_running and snapshot["acquisition_connected"]
                if service_running:
                    received = snapshot.get("last_received_timestamp") or received
                    cycle = snapshot.get("last_cycle") or cycle
                    if snapshot.get("ups"):
                        ups_status = UpsStatus(**snapshot["ups"])
            except (ValueError, KeyError, TypeError):
                pass
        try:
            parsed_report_timing = json.loads(report_timing) if report_timing else None
        except json.JSONDecodeError:
            parsed_report_timing = {"unparseable": report_timing}
        return {
            "application_version": __version__,
            "configuration_version": self.configuration.config_version,
            "application_state": "shutdown_pending"
            if self.shutdown_pending
            else "running"
            if service_running
            else "stopped_or_stale",
            "current_report_period_id": open_period["report_id"] if open_period else None,
            "current_report_period_status": open_period["state"] if open_period else None,
            "acquisition_hours": accumulated_seconds / 3600,
            "hours_until_next_report": max(
                0.0, self.configuration.reporting.acquisition_hours - accumulated_seconds / 3600
            ),
            "acquisition_connected": connected,
            "acquisition_state": "connected" if connected else "disconnected_or_idle",
            "acquisition_provider": self.configuration.raw.get("acquisition", {}).get(
                "provider", "none"
            ),
            "last_acquired_timestamp": timestamp,
            "last_pi_receive_timestamp": received,
            "last_cycle": cycle,
            "records_in_current_period": records,
            "sealed_reports_awaiting_generation": sum(
                report_counts.get(state, 0)
                for state in (
                    ReportPeriodState.DUE.value,
                    ReportPeriodState.SEALED.value,
                    ReportPeriodState.REPORT_BUILDING.value,
                )
            ),
            "pending_email_reports": email_counts.get("PENDING", 0)
            + email_counts.get("FAILED_RETRYABLE", 0),
            "ambiguous_emails": email_counts.get("AMBIGUOUS", 0),
            "report_state_counts": report_counts,
            "email_state_counts": email_counts,
            "last_successful_report": last_report["report_id"] if last_report else None,
            "last_successful_email": last_email["email_sent_at"] if last_email else None,
            "disk_free_bytes": health.disk_free_bytes,
            "disk_used_percent": health.disk_used_percent,
            "cpu_temperature_c": health.cpu_temperature_c,
            "thermal_throttled": health.thermal_throttled,
            "ntp_status": health.ntp_synchronized,
            "ups_state": "not configured"
            if ups_status is None
            else {
                "external_power_present": ups_status.external_power_present,
                "battery_critical": ups_status.battery_critical,
                "battery_percent": ups_status.battery_percent,
            },
            "sqlite_durability": self.database.durability_settings(),
            "last_report_timing": parsed_report_timing,
        }

    def _checkpoint_observation(self) -> None:
        now = time.monotonic()
        if now - self._last_observation_checkpoint < 60:
            return
        self._last_observation_checkpoint = now
        if self._last_acquired_timestamp is not None:
            self.database.set_application_state(
                "last_acquired_timestamp", self._last_acquired_timestamp.isoformat()
            )
        if self._last_received_timestamp is not None:
            self.database.set_application_state(
                "last_received_timestamp", self._last_received_timestamp.isoformat()
            )
        if self._last_cycle is not None:
            self.database.set_application_state("last_cycle", self._last_cycle)
