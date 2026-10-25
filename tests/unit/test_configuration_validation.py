"""Synthetic fixtures only; no operational datasets are required."""

from pathlib import Path

import pytest

from edge_logger.config import ConfigurationError, load_config


def _write_config(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def _base() -> str:
    return """
machine:
  name: DEMO-01
  timezone: Europe/London
application:
  config_version: 1
reporting:
  trigger_mode: acquisition_hours
  acquisition_hours: 120
  maximum_calendar_days: null
  trend_bucket_minutes: 5
  trend_bucket_count: 30
  generate_if_due_on_startup: true
paths:
  raw: raw
  reports: reports
  state: state
retention:
  days_after_successful_email: 30
ups:
  shutdown_delay_seconds: 90
email:
  enabled: false
  port: 587
  password: null
opcua:
  endpoint: null
signals: {}
"""


@pytest.mark.parametrize(
    ("changed", "field"),
    [
        ("acquisition_hours: 0", "reporting.acquisition_hours"),
        ("trend_bucket_count: 0", "reporting.trend_bucket_count"),
        ("days_after_successful_email: 0", "retention.days_after_successful_email"),
        ("timezone: Never/Here", "machine.timezone"),
        ("port: 70000", "email.port"),
    ],
)
def test_rejects_invalid_scalar_configuration(tmp_path: Path, changed: str, field: str) -> None:
    baseline = _base()
    key = changed.split(":", maxsplit=1)[0]
    value = changed.split(":", maxsplit=1)[1]
    modified = baseline.replace(
        f"{key}:" + baseline.split(f"{key}:", 1)[1].split("\n", 1)[0], f"{key}:{value}", 1
    )
    with pytest.raises(ConfigurationError, match=field):
        load_config(_write_config(tmp_path, modified))


def test_rejects_duplicate_yaml_signal_id(tmp_path: Path) -> None:
    content = _base().replace(
        "signals: {}",
        "signals:\n  cycle_time:\n    source_name: Cycle time\n  cycle_time:\n    source_name: Cycle time 2",
    )
    with pytest.raises(ConfigurationError, match="Duplicate configuration key"):
        load_config(_write_config(tmp_path, content))


def test_rejects_duplicate_node_ids_and_required_signal_without_mapping(tmp_path: Path) -> None:
    duplicate = _base().replace(
        "signals: {}",
        "signals:\n  one:\n    node_id: ns=2;i=1\n  two:\n    node_id: ns=2;i=1",
    )
    with pytest.raises(ConfigurationError, match="Duplicate signal NodeId"):
        load_config(_write_config(tmp_path, duplicate))
    required = _base().replace("signals: {}", "signals:\n  one:\n    required: true")
    with pytest.raises(ConfigurationError, match="signals.one"):
        load_config(_write_config(tmp_path, required))


def test_rejects_control_configuration_and_incomplete_opcua(tmp_path: Path) -> None:
    control = _base().replace("  endpoint: null", "  endpoint: null\n  write_value: ns=2;i=1")
    with pytest.raises(ConfigurationError, match="read-only"):
        load_config(_write_config(tmp_path, control))
    opcua = (
        _base()
        .replace(
            "signals: {}",
            "signals:\n  cycle:\n    node_id: ns=2;i=1\n    is_cycle_id: true",
        )
        .replace("retention:\n", "acquisition:\n  provider: opcua\nretention:\n")
        .replace("  endpoint: null", "  endpoint: invalid")
    )
    with pytest.raises(ConfigurationError, match="opcua.endpoint"):
        load_config(_write_config(tmp_path, opcua))


def test_demo_long_cycle_rule_can_be_intentionally_disabled(tmp_path: Path) -> None:
    configuration = _base().replace("email:\n", "rules:\n  long_cycle_seconds: null\nemail:\n")
    assert load_config(_write_config(tmp_path, configuration)).rules["long_cycle_seconds"] is None


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ("  generate_if_due_on_startup: 1", "generate_if_due_on_startup"),
        ("  enabled: 1", "email.enabled"),
        ("  password: secret", "email.password"),
    ],
)
def test_rejects_ambiguous_boolean_and_plaintext_email_password(
    tmp_path: Path, replacement: str, message: str
) -> None:
    baseline = _base()
    key = replacement.strip().split(":", maxsplit=1)[0]
    current = next(line for line in baseline.splitlines() if line.strip().startswith(f"{key}:"))
    with pytest.raises(ConfigurationError, match=message):
        load_config(_write_config(tmp_path, baseline.replace(current, replacement, 1)))
