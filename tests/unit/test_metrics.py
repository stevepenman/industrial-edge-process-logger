"""Synthetic fixtures only; no operational datasets are required."""

import sys

from edge_logger.metrics import PipelineMetrics


def test_peak_memory_measurement_is_available_on_supported_platforms() -> None:
    metrics = PipelineMetrics()
    metrics.observe_memory()
    if sys.platform in {"win32", "darwin", "linux"}:
        assert metrics.peak_rss_bytes is not None and metrics.peak_rss_bytes > 0
