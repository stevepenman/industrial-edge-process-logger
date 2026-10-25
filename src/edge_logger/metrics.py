"""Small, dependency-free timing and peak-memory instrumentation."""

from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass(slots=True)
class PipelineMetrics:
    """Accumulate named stages without retaining individual record timings."""

    phases_seconds: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    peak_rss_bytes: int | None = None
    _last_memory_sample: float = field(default=0.0, init=False, repr=False)

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        started = time.perf_counter()
        try:
            yield
        finally:
            self.phases_seconds[name] += time.perf_counter() - started
            self.observe_memory()

    def observe_memory(self) -> None:
        now = time.perf_counter()
        if now - self._last_memory_sample < 0.25:
            return
        self._last_memory_sample = now
        value = _peak_rss_bytes()
        if value is not None:
            self.peak_rss_bytes = max(self.peak_rss_bytes or 0, value)

    def as_dict(self) -> dict[str, object]:
        return {
            "phase_seconds": {
                name: round(value, 6) for name, value in sorted(self.phases_seconds.items())
            },
            "peak_rss_bytes": self.peak_rss_bytes,
        }


def _peak_rss_bytes() -> int | None:
    """Return the process peak resident set/working-set size when the OS exposes it."""
    if sys.platform == "win32":
        return _windows_peak_working_set()
    try:
        import resource

        value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        return value if os.uname().sysname == "Darwin" else value * 1024
    except (ImportError, AttributeError):
        return None


def _windows_peak_working_set() -> int | None:
    """Use the native process API rather than adding a profiling-only dependency."""
    if sys.platform != "win32":
        return None
    try:
        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCountersEx(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("page_fault_count", wintypes.DWORD),
                ("peak_working_set_size", ctypes.c_size_t),
                ("working_set_size", ctypes.c_size_t),
                ("quota_peak_paged_pool_usage", ctypes.c_size_t),
                ("quota_paged_pool_usage", ctypes.c_size_t),
                ("quota_peak_non_paged_pool_usage", ctypes.c_size_t),
                ("quota_non_paged_pool_usage", ctypes.c_size_t),
                ("pagefile_usage", ctypes.c_size_t),
                ("peak_pagefile_usage", ctypes.c_size_t),
                ("private_usage", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCountersEx),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        counters = ProcessMemoryCountersEx()
        counters.cb = ctypes.sizeof(counters)
        process = kernel32.GetCurrentProcess()
        success = psapi.GetProcessMemoryInfo(process, ctypes.byref(counters), counters.cb)
        return int(counters.peak_working_set_size) if success else None
    except (AttributeError, OSError):
        return None
