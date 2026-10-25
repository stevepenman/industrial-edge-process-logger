"""Safe command-line operations for the DEMO-01 logger."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sqlite3
import sys
from collections.abc import Coroutine
from contextlib import ExitStack
from pathlib import Path
from typing import Any

from .acquisition.base import AcquisitionProvider
from .acquisition.opcua import OpcUaProvider
from .config import ConfigurationError, load_config
from .reporting.validator import validate_workbook
from .runtime import LoggerRuntime
from .storage.audit_db import AuditDatabase
from .storage.lock import AcquisitionLock
from .storage.receiver import ingest_export
from .ups.command import CommandUpsProvider, request_host_shutdown


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="edge-logger")
    parser.add_argument("--config", type=Path, default=Path("/etc/edge-logger/config.yaml"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run")
    status = commands.add_parser("status")
    status.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    commands.add_parser("db-check")
    importer = commands.add_parser("import")
    importer.add_argument("file", type=Path)
    importer.add_argument("--dry-run", action="store_true")
    replay = commands.add_parser("replay")
    replay.add_argument("file", type=Path)
    replay.add_argument("--force-report", action="store_true")
    replay.add_argument("--speed", type=float, default=1.0)
    retry = commands.add_parser("retry-email")
    retry.add_argument("report_id")
    retry_report = commands.add_parser("retry-report")
    retry_report.add_argument("report_id")
    commands.add_parser("email-test")
    retention = commands.add_parser("retention-check")
    retention.add_argument(
        "--apply", action="store_true", help="Delete only retention-eligible files"
    )
    validator = commands.add_parser("validate-report")
    validator.add_argument("file", type=Path)
    commands.add_parser("browse-opc")
    exporter = commands.add_parser("export-records")
    exporter.add_argument("report_id")
    receiver = commands.add_parser("ingest-export")
    receiver.add_argument("file", type=Path)
    receiver.add_argument("--database", required=True, type=Path)
    acknowledgement = commands.add_parser("acknowledge-storage")
    acknowledgement.add_argument("report_id")
    acknowledgement.add_argument("--sha256", required=True)
    acknowledgement.add_argument("--records-sha256")
    acknowledgement.add_argument("--receipt", required=True)
    backup = commands.add_parser("backup-db")
    backup.add_argument("destination", type=Path)
    args = parser.parse_args(arguments)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        configuration = load_config(args.config)
        if args.command == "db-check":
            check = AuditDatabase.read_only_check(configuration.state_path / "state.sqlite3")
            print(json.dumps(check, indent=2, default=str))
            return 0 if check["valid"] else 2
        if args.command == "ingest-export":
            print(json.dumps(ingest_export(args.file, args.database), indent=2))
            return 0
        with ExitStack() as resources:
            if args.command in {
                "run",
                "import",
                "replay",
                "retry-report",
                "export-records",
                "retention-check",
            }:
                resources.enter_context(AcquisitionLock(configuration.state_path))
            database = AuditDatabase(configuration.state_path / "state.sqlite3")
            resources.callback(database.close)
            runtime = LoggerRuntime(configuration, database)
            if args.command == "run":
                ups = configuration.raw.get("ups", {})
                if ups.get("provider") == "command":
                    runtime.ups_provider = CommandUpsProvider(
                        ups["status_command"], float(ups.get("status_timeout_seconds", 3))
                    )
                    runtime.shutdown_callback = lambda: request_host_shutdown(
                        configuration.state_path
                    )
                _run(runtime, _provider(configuration))
            elif args.command == "status":
                _print_status(runtime.status(), args.json)
            elif args.command == "import":
                _print_import(_run_async(runtime.import_file(args.file, dry_run=args.dry_run)))
            elif args.command == "replay":
                if args.speed <= 0:
                    raise ValueError("replay --speed must be positive")
                _print_import(
                    _run_async(runtime.import_file(args.file, force_report=args.force_report))
                )
            elif args.command == "retry-email":
                email_result = runtime.retry_email(args.report_id)
                print(
                    json.dumps(
                        {
                            "report_id": email_result.report_id,
                            "state": email_result.state,
                            "detail": email_result.detail,
                        }
                    )
                )
            elif args.command == "retry-report":
                _run_async(runtime.retry_report(args.report_id))
                print(
                    json.dumps(
                        {
                            "report_id": args.report_id,
                            "state": database.report_period(args.report_id)["state"],
                        }
                    )
                )
            elif args.command == "email-test":
                print(json.dumps(runtime.email_test(), indent=2))
            elif args.command == "retention-check":
                results = runtime.retention_check(dry_run=not args.apply)
                print(
                    json.dumps(
                        [
                            {
                                "report_id": item.report_id,
                                "eligible": item.eligible,
                                "deleted_paths": [str(path) for path in item.deleted_paths],
                                "reason": item.reason,
                            }
                            for item in results
                        ],
                        indent=2,
                    )
                )
            elif args.command == "validate-report":
                validation = validate_workbook(args.file)
                print(json.dumps({"valid": validation.valid, "errors": validation.errors}))
                return 0 if validation.valid else 2
            elif args.command == "browse-opc":
                _browse_opc(configuration)
            elif args.command == "export-records":
                print(json.dumps(runtime.export_period(args.report_id), indent=2))
            elif args.command == "acknowledge-storage":
                database.acknowledge_storage(
                    args.report_id, args.sha256, args.receipt, args.records_sha256
                )
                print(json.dumps({"report_id": args.report_id, "storage_acknowledged": True}))
            elif args.command == "backup-db":
                if args.destination.exists():
                    raise ValueError("Backup destination already exists")
                args.destination.parent.mkdir(parents=True, exist_ok=True)
                with sqlite3.connect(args.destination) as destination:
                    database.connection.backup(destination)
                print(json.dumps(AuditDatabase.read_only_check(args.destination), indent=2))
    except (ConfigurationError, OSError, RuntimeError, ValueError, sqlite3.Error) as error:
        print(f"edge-logger: {error}", file=sys.stderr)
        return 2
    return 0


def _run(runtime: LoggerRuntime, provider: AcquisitionProvider | None = None) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(runtime.request_stop()))
            except NotImplementedError:
                signal.signal(
                    sig,
                    lambda *_: loop.call_soon_threadsafe(
                        lambda: asyncio.create_task(runtime.request_stop())
                    ),
                )
        loop.run_until_complete(runtime.run(provider))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())
        loop.close()


def _run_async(coroutine: Coroutine[Any, Any, Any]) -> Any:
    return asyncio.run(coroutine)


def _print_import(result: object) -> None:
    inspection = result.inspection  # type: ignore[attr-defined]
    print(
        json.dumps(
            {
                "checksum": result.checksum,  # type: ignore[attr-defined]
                "duplicate": result.duplicate,  # type: ignore[attr-defined]
                "imported_records": result.imported_records,  # type: ignore[attr-defined]
                "sealed_report_ids": result.sealed_report_ids,  # type: ignore[attr-defined]
                "metrics": result.metrics,  # type: ignore[attr-defined]
                "inspection": {
                    "encoding": inspection.encoding,
                    "delimiter": inspection.delimiter,
                    "row_count": inspection.row_count,
                    "columns": inspection.columns,
                    "timestamp_column": inspection.timestamp_column,
                    "date_column": inspection.date_column,
                    "time_column": inspection.time_column,
                    "cycle_column": inspection.cycle_column,
                    "known_signals": inspection.known_signals,
                    "unknown_signals": inspection.unknown_signals,
                    "parse_issue_count": inspection.parse_issue_count,
                    "physical_column_count": inspection.physical_column_count,
                    "named_column_count": inspection.named_column_count,
                    "ignored_blank_trailing_columns": inspection.ignored_blank_trailing_columns,
                    "source_ordering": inspection.source_ordering,
                },
            },
            indent=2,
            default=str,
        )
    )


def _print_status(status: dict[str, object], as_json: bool) -> None:
    if as_json:
        print(json.dumps(status, indent=2, default=str))
        return
    for key, value in status.items():
        print(f"{key}: {value}")


def _browse_opc(configuration: object) -> None:
    opcua = configuration.raw.get("opcua", {})  # type: ignore[attr-defined]
    endpoint = opcua.get("endpoint")
    if not endpoint:
        raise ConfigurationError("opcua.endpoint is required for browse-opc; no value is guessed")
    provider = OpcUaProvider.from_configuration(configuration)
    print(json.dumps(asyncio.run(provider.browse_root()), indent=2))


def _provider(configuration: object) -> OpcUaProvider | None:
    acquisition = configuration.raw.get("acquisition", {})  # type: ignore[attr-defined]
    if acquisition.get("provider") != "opcua":
        return None
    return OpcUaProvider.from_configuration(configuration)
