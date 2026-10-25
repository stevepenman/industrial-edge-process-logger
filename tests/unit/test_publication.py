"""Synthetic examples for the publication guardrail."""

from tools.check_publication import findings


def test_operational_files_and_live_config_are_flagged():
    assert findings("reports/report.xlsx", "")
    assert findings("state/cycles.sqlite3", "")
    assert findings("config/local.yaml", "email:\n  host: smtp.example.test\n")


def test_only_explicit_synthetic_preview_is_allowed_as_csv():
    assert findings("examples/synthetic_preview.csv", "cycle,pressure\n1000,100\n") == []
    assert findings("examples/arbitrary.csv", "cycle,pressure\n1000,100\n")


def test_unconfigured_integrations_and_loopback_are_allowed():
    assert (
        findings("config/config.example.yaml", "opcua:\n  endpoint: null\nemail:\n  host: null\n")
        == []
    )
    assert findings("tools/simulator.py", "endpoint = 'opc.tcp://127.0.0.1:48410/demo/'") == []


def test_token_detection_uses_a_constructed_dummy_value():
    dummy = "gh" + "p_" + "X" * 36
    assert "access token" in findings("example.txt", dummy)
