"""Versioned YAML configuration with fail-closed validation for commissioning values."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml  # type: ignore[import-untyped]


class ConfigurationError(ValueError):
    """Configuration cannot safely start the application."""


class _UniqueKeyLoader(yaml.SafeLoader):  # type: ignore[misc]
    """Reject duplicate YAML keys instead of silently keeping the last value."""


def _construct_unique_mapping(
    loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[object, object]:
    mapping: dict[object, object] = {}
    for key_node, value_node in node.value:
        if key_node.tag == "tag:yaml.org,2002:merge":
            merged = loader.construct_object(value_node, deep=True)
            merge_items = merged if isinstance(merged, list) else [merged]
            for item in merge_items:
                if not isinstance(item, dict):
                    raise ConfigurationError("YAML merge value must be a mapping")
                for merge_key, merge_value in item.items():
                    mapping.setdefault(merge_key, merge_value)
            continue
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ConfigurationError(f"Duplicate configuration key: {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping
)


@dataclass(frozen=True, slots=True)
class ReportingConfig:
    trigger_mode: str
    acquisition_hours: float
    maximum_calendar_days: float | None
    trend_bucket_minutes: int
    generate_if_due_on_startup: bool
    minimum_records: int
    trend_bucket_count: int = 30
    maximum_records: int = 20_000


@dataclass(frozen=True, slots=True)
class AppConfig:
    machine_name: str
    timezone: str
    config_version: int
    reporting: ReportingConfig
    raw_path: Path
    reports_path: Path
    state_path: Path
    rules: dict[str, Any]
    signal_registry: dict[str, dict[str, Any]]
    raw: dict[str, Any]


def load_config(path: Path) -> AppConfig:
    try:
        loaded = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except (OSError, yaml.YAMLError) as error:
        raise ConfigurationError(f"Cannot read configuration {path}: {error}") from error
    if not isinstance(loaded, dict):
        raise ConfigurationError("Configuration root must be a mapping")
    try:
        machine = _mapping(loaded, "machine")
        application = _mapping(loaded, "application")
        reporting = _mapping(loaded, "reporting")
        paths = _mapping(loaded, "paths")
        timezone = _string(machine, "timezone")
        try:
            ZoneInfo(timezone)
        except ZoneInfoNotFoundError as error:
            raise ConfigurationError(f"machine.timezone is unsupported: {timezone}") from error
        trigger_mode = _string(reporting, "trigger_mode")
        if trigger_mode not in {"acquisition_hours", "machine_production_hours"}:
            raise ConfigurationError(
                "reporting.trigger_mode must be acquisition_hours or machine_production_hours"
            )
        acquisition_hours = _positive_number(reporting, "acquisition_hours")
        if acquisition_hours <= 0:
            raise ConfigurationError("reporting.acquisition_hours must be positive")
        trend_bucket_minutes = _positive_integer(reporting, "trend_bucket_minutes")
        trend_bucket_count = _positive_integer(reporting, "trend_bucket_count", default=30)
        maximum_calendar_days = _optional_float(reporting.get("maximum_calendar_days"))
        if maximum_calendar_days is not None and (
            not math.isfinite(maximum_calendar_days) or maximum_calendar_days <= 0
        ):
            raise ConfigurationError(
                "reporting.maximum_calendar_days must be positive when configured"
            )
        minimum_records = _positive_integer(reporting, "minimum_records", default=1)
        maximum_records = _positive_integer(reporting, "maximum_records", default=20_000)
        if maximum_records > 1_000_000 or minimum_records > maximum_records:
            raise ConfigurationError(
                "reporting.maximum_records must be between minimum_records and 1000000"
            )
        config_version = _positive_integer(application, "config_version")
        signal_registry = _signal_registry(loaded)
        _validate_operational_configuration(loaded, signal_registry)
        return AppConfig(
            machine_name=_string(machine, "name"),
            timezone=timezone,
            config_version=config_version,
            reporting=ReportingConfig(
                trigger_mode=trigger_mode,
                acquisition_hours=acquisition_hours,
                maximum_calendar_days=maximum_calendar_days,
                trend_bucket_minutes=trend_bucket_minutes,
                generate_if_due_on_startup=_boolean(reporting, "generate_if_due_on_startup"),
                minimum_records=minimum_records,
                trend_bucket_count=trend_bucket_count,
                maximum_records=maximum_records,
            ),
            raw_path=Path(_string(paths, "raw")),
            reports_path=Path(_string(paths, "reports")),
            state_path=Path(_string(paths, "state")),
            rules=dict(loaded.get("rules", {})),
            signal_registry=signal_registry,
            raw=loaded,
        )
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, ConfigurationError):
            raise
        raise ConfigurationError(f"Invalid configuration: {error}") from error


def _mapping(parent: dict[str, Any], key: str) -> dict[str, Any]:
    value = parent[key]
    if not isinstance(value, dict):
        raise ConfigurationError(f"{key} must be a mapping")
    return value


def _string(parent: dict[str, Any], key: str) -> str:
    value = parent[key]
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{key} must be a non-empty string")
    return value


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ConfigurationError("reporting.maximum_calendar_days must be a number or null")
    return float(value)


def _positive_number(parent: dict[str, Any], key: str) -> float:
    value = parent[key]
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ConfigurationError(f"reporting.{key} must be a positive number")
    return float(value)


def _positive_integer(parent: dict[str, Any], key: str, default: int | None = None) -> int:
    value = parent.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        prefix = "application" if parent is not None and key == "config_version" else "reporting"
        raise ConfigurationError(f"{prefix}.{key} must be a positive integer")
    return value


def _boolean(parent: dict[str, Any], key: str) -> bool:
    value = parent[key]
    if not isinstance(value, bool):
        raise ConfigurationError(f"reporting.{key} must be true or false")
    return value


def _signal_registry(loaded: dict[str, Any]) -> dict[str, dict[str, Any]]:
    configured = loaded.get("signals", {})
    if not isinstance(configured, dict):
        raise ConfigurationError("signals must be a mapping")
    node_ids: set[str] = set()
    result: dict[str, dict[str, Any]] = {}
    for signal_id, metadata in configured.items():
        field = f"signals.{signal_id}"
        if not isinstance(signal_id, str) or not signal_id:
            raise ConfigurationError("signals contains an empty or non-string signal ID")
        if not isinstance(metadata, dict):
            raise ConfigurationError(f"{field} must be a mapping")
        item = dict(metadata)
        for flag in ("required", "is_cycle_id", "cycle_synchronous", "is_running_state", "trend"):
            if flag in item and type(item[flag]) is not bool:
                raise ConfigurationError(f"{field}.{flag} must be true or false")
        if item.get("bad_part_mode", "indicator") not in {"indicator", "quantity"}:
            raise ConfigurationError(
                f"{field}.bad_part_mode must be indicator or quantity; cumulative counters require an OEM adapter"
            )
        source_name = item.get("source_name")
        if item.get("production_field") is not None and item["production_field"] not in {
            "job_id",
            "product_id",
            "mould_id",
            "material_batch",
            "active_cavities",
            "sample_parts",
            "rejected_parts",
            "run_id",
        }:
            raise ConfigurationError(f"{field}.production_field is unsupported")
        node_id = item.get("node_id")
        if bool(item.get("required", False)) and not source_name and not node_id:
            raise ConfigurationError(f"{field} is required but has neither source_name nor node_id")
        if source_name is not None and (
            not isinstance(source_name, str) or not source_name.strip()
        ):
            raise ConfigurationError(f"{field}.source_name must be a non-empty string")
        if node_id is not None:
            if not isinstance(node_id, str) or not node_id.strip():
                raise ConfigurationError(f"{field}.node_id must be a non-empty string")
            if node_id in node_ids:
                raise ConfigurationError(f"Duplicate signal NodeId: {node_id}")
            node_ids.add(node_id)
        result[signal_id] = item
    return result


def _validate_operational_configuration(
    loaded: dict[str, Any], signal_registry: dict[str, dict[str, Any]]
) -> None:
    acquisition = loaded.get("acquisition", {})
    if not isinstance(acquisition, dict):
        raise ConfigurationError("acquisition must be a mapping")
    provider = acquisition.get("provider", "none")
    if provider not in {"none", "replay", "opcua"}:
        raise ConfigurationError("acquisition.provider must be none, replay, or opcua")
    if acquisition.get("communication_gap_seconds") is not None:
        _finite_setting(acquisition, "communication_gap_seconds", 1, "acquisition")
    production = loaded.get("production", {})
    if not isinstance(production, dict):
        raise ConfigurationError("production must be a mapping")
    cavities = production.get("active_cavities")
    if cavities is not None and (type(cavities) is not int or cavities < 1):
        raise ConfigurationError("production.active_cavities must be a positive integer or null")
    operations = loaded.get("operations", {})
    if not isinstance(operations, dict):
        raise ConfigurationError("operations must be a mapping")
    for field, default in (
        ("poll_seconds", 30),
        ("stale_data_seconds", 300),
        ("minimum_free_disk_bytes", 1024**3),
    ):
        _finite_setting(operations, field, default, "operations")
    if type(operations.get("automatic_retention", True)) is not bool:
        raise ConfigurationError("operations.automatic_retention must be true or false")
    retention = loaded.get("retention", {})
    if not isinstance(retention, dict):
        raise ConfigurationError("retention must be a mapping")
    rules = loaded.get("rules", {})
    if not isinstance(rules, dict):
        raise ConfigurationError("rules must be a mapping")
    long_cycle = rules.get("long_cycle_seconds")
    if long_cycle is not None and (
        not isinstance(long_cycle, (int, float))
        or isinstance(long_cycle, bool)
        or not math.isfinite(long_cycle)
        or long_cycle <= 0
    ):
        raise ConfigurationError("rules.long_cycle_seconds must be a positive number or null")
    source_gap = rules.get("source_data_gap_seconds", 60)
    if (
        not isinstance(source_gap, (int, float))
        or isinstance(source_gap, bool)
        or not math.isfinite(source_gap)
        or source_gap <= 0
    ):
        raise ConfigurationError("rules.source_data_gap_seconds must be a positive number")
    days = retention.get("days_after_successful_email", 30)
    if not isinstance(days, int) or isinstance(days, bool) or days < 1:
        raise ConfigurationError(
            "retention.days_after_successful_email must be an integer of at least 1"
        )
    ups = loaded.get("ups", {})
    if not isinstance(ups, dict):
        raise ConfigurationError("ups must be a mapping")
    delay = ups.get("shutdown_delay_seconds", 90)
    if (
        not isinstance(delay, (int, float))
        or isinstance(delay, bool)
        or not math.isfinite(delay)
        or delay < 0
    ):
        raise ConfigurationError("ups.shutdown_delay_seconds must be a non-negative number")
    if ups.get("provider", "none") not in {"none", "command"}:
        raise ConfigurationError(
            "ups.provider must be none or command; use a verified vendor utility"
        )
    if ups.get("provider") == "command":
        command = ups.get("status_command")
        if (
            not isinstance(command, list)
            or not command
            or not all(isinstance(item, str) and item for item in command)
        ):
            raise ConfigurationError("ups.status_command must be a non-empty argument list")
        if not Path(command[0]).is_absolute():
            raise ConfigurationError("ups.status_command executable must be an absolute path")
    _finite_setting(ups, "status_timeout_seconds", 3, "ups")
    email = loaded.get("email", {})
    if not isinstance(email, dict):
        raise ConfigurationError("email must be a mapping")
    port = email.get("port", 587)
    _finite_setting(email, "timeout_seconds", 30, "email")
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise ConfigurationError("email.port must be an integer between 1 and 65535")
    if email.get("tls_mode", "starttls") not in {"starttls", "ssl", "none"}:
        raise ConfigurationError("email.tls_mode must be starttls, ssl, or none")
    attachment_limit = email.get("max_attachment_bytes", 18 * 1024 * 1024)
    message_limit = email.get("max_message_bytes", 25 * 1024 * 1024)
    if type(message_limit) is not int or message_limit < 1:
        raise ConfigurationError("email.max_message_bytes must be a positive integer")
    if (
        not isinstance(attachment_limit, int)
        or isinstance(attachment_limit, bool)
        or attachment_limit <= 0
    ):
        raise ConfigurationError("email.max_attachment_bytes must be a positive integer")
    enabled = email.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ConfigurationError("email.enabled must be true or false")
    if email.get("password") is not None:
        raise ConfigurationError("email.password is forbidden; use email.password_env")
    if enabled:
        for field in ("host", "sender"):
            if not isinstance(email.get(field), str) or not str(email[field]).strip():
                raise ConfigurationError(f"email.{field} is required when email.enabled is true")
        recipients = email.get("recipients")
        if (
            not isinstance(recipients, list)
            or not recipients
            or not all(isinstance(value, str) and value.strip() for value in recipients)
        ):
            raise ConfigurationError(
                "email.recipients must contain one or more addresses when email.enabled is true"
            )
    for field in ("username", "password_env"):
        if email.get(field) is not None and (
            not isinstance(email[field], str) or not email[field].strip()
        ):
            raise ConfigurationError(f"email.{field} must be a non-empty string when configured")
    opcua = loaded.get("opcua", {})
    if not isinstance(opcua, dict):
        raise ConfigurationError("opcua must be a mapping")
    _reject_control_configuration(opcua, "opcua")
    if opcua.get("password") is not None:
        raise ConfigurationError("opcua.password is forbidden; use opcua.password_env")
    if provider != "opcua":
        if loaded["reporting"].get("trigger_mode") == "machine_production_hours":
            raise ConfigurationError(
                "machine_production_hours requires OPC UA and a running-state signal"
            )
        return
    endpoint = opcua.get("endpoint")
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise ConfigurationError("opcua.endpoint is required when acquisition.provider is opcua")
    try:
        parsed = urlparse(endpoint)
        valid_endpoint = parsed.scheme == "opc.tcp" and parsed.hostname and parsed.port is not None
    except ValueError:
        valid_endpoint = False
    if not valid_endpoint:
        raise ConfigurationError("opcua.endpoint must be an opc.tcp URL with host and port")
    interval = opcua.get("subscription_interval_ms")
    if not isinstance(interval, int) or isinstance(interval, bool) or interval <= 0:
        raise ConfigurationError("opcua.subscription_interval_ms must be a positive integer")
    for field in ("reconnect_initial_seconds", "reconnect_max_seconds"):
        value = opcua.get(field, 1.0)
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ConfigurationError(f"opcua.{field} must be a positive number")
    if float(opcua.get("reconnect_max_seconds", 30.0)) < float(
        opcua.get("reconnect_initial_seconds", 1.0)
    ):
        raise ConfigurationError(
            "opcua.reconnect_max_seconds must be at least reconnect_initial_seconds"
        )
    queue_size = opcua.get("queue_size", 1_000)
    if not isinstance(queue_size, int) or isinstance(queue_size, bool) or queue_size < 1:
        raise ConfigurationError("opcua.queue_size must be a positive integer")
    monitored_queue = opcua.get("monitored_item_queue_size", 1_000)
    if type(monitored_queue) is not int or monitored_queue < 2:
        raise ConfigurationError("opcua.monitored_item_queue_size must be an integer of at least 2")
    for field, default in (("snapshot_wait_seconds", 2), ("asynchronous_max_age_seconds", 60)):
        _finite_setting(opcua, field, default, "opcua")
    tolerance = opcua.get("snapshot_timestamp_tolerance_seconds", 0)
    if (
        not isinstance(tolerance, (float, int))
        or isinstance(tolerance, bool)
        or not math.isfinite(tolerance)
        or tolerance < 0
    ):
        raise ConfigurationError(
            "opcua.snapshot_timestamp_tolerance_seconds must be finite and non-negative"
        )
    for field in (
        "username",
        "password_env",
        "client_certificate",
        "private_key",
        "trusted_server_certificate",
    ):
        if opcua.get(field) is not None and (
            not isinstance(opcua[field], str) or not opcua[field].strip()
        ):
            raise ConfigurationError(f"opcua.{field} must be a non-empty string when configured")
    if opcua.get("username") and not opcua.get("password_env"):
        raise ConfigurationError("opcua.username requires opcua.password_env")
    mode = opcua.get("security_mode", "None")
    if mode not in {None, "None", "Sign", "SignAndEncrypt"}:
        raise ConfigurationError("opcua.security_mode must be None, Sign, or SignAndEncrypt")
    policy = opcua.get("security_policy", "None")
    if policy not in {None, "None", "Basic256Sha256", "Aes128Sha256RsaOaep", "Aes256Sha256RsaPss"}:
        raise ConfigurationError("opcua.security_policy is unsupported")
    if (mode in {None, "None"}) != (policy in {None, "None"}):
        raise ConfigurationError(
            "opcua security policy and mode must both enable security or both be None"
        )
    if mode not in {None, "None"} and not (
        opcua.get("client_certificate")
        and opcua.get("private_key")
        and opcua.get("trusted_server_certificate")
    ):
        raise ConfigurationError(
            "opcua client_certificate, private_key, and trusted_server_certificate are required for secure mode"
        )
    for signal_id, metadata in signal_registry.items():
        if bool(metadata.get("required", False)) and not metadata.get("node_id"):
            raise ConfigurationError(
                f"signals.{signal_id}.node_id is required for OPC UA acquisition"
            )
    if (
        sum(
            bool(metadata.get("is_cycle_id") and metadata.get("node_id"))
            for metadata in signal_registry.values()
        )
        != 1
    ):
        raise ConfigurationError(
            "signals requires exactly one is_cycle_id signal with node_id for OPC UA acquisition"
        )
    if (
        loaded["reporting"].get("trigger_mode") == "machine_production_hours"
        and sum(
            bool(metadata.get("is_running_state") and metadata.get("node_id"))
            for metadata in signal_registry.values()
        )
        != 1
    ):
        raise ConfigurationError(
            "machine_production_hours requires exactly one is_running_state signal with node_id"
        )


def _finite_setting(mapping: dict[str, Any], key: str, default: float, prefix: str) -> float:
    value = mapping.get(key, default)
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ConfigurationError(f"{prefix}.{key} must be finite and positive")
    return float(value)


def _reject_control_configuration(value: object, field: str) -> None:
    if not isinstance(value, dict):
        return
    forbidden = {
        "write",
        "write_value",
        "set_value",
        "write_attribute",
        "call_method",
        "control",
        "method",
    }
    for key, child in value.items():
        current = f"{field}.{key}"
        if str(key).lower() in forbidden:
            raise ConfigurationError(
                f"{current} is forbidden: DEMO-01 OPC UA configuration is read-only"
            )
        _reject_control_configuration(child, current)
