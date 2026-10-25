"""Per-period clean-constant signal classification."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from ..models import ProcessRecord, QualityStatus


@dataclass(frozen=True, slots=True)
class SignalClassification:
    signal_id: str
    display_name: str
    value: object | None
    unit: str | None
    record_count: int
    is_clean_constant: bool
    quality_summary: str


def classify_signals(records: Iterable[ProcessRecord]) -> dict[str, SignalClassification]:
    state: dict[str, dict[str, Any]] = {}
    for record in records:
        for signal in record.signals:
            entry = state.setdefault(
                signal.signal_id,
                {
                    "name": signal.display_name,
                    "unit": signal.unit,
                    "count": 0,
                    "first": None,
                    "set": False,
                    "clean": True,
                    "qualities": set(),
                },
            )
            entry["count"] = int(entry["count"]) + 1
            qualities = entry["qualities"]
            assert isinstance(qualities, set)
            qualities.add(signal.quality.value)
            if signal.value is None or signal.quality is not QualityStatus.GOOD:
                entry["clean"] = False
            elif not bool(entry["set"]):
                entry["first"] = signal.value
                entry["set"] = True
            elif signal.value != entry["first"]:
                entry["clean"] = False
    return {
        signal_id: SignalClassification(
            signal_id=signal_id,
            display_name=str(entry["name"]),
            value=entry["first"],
            unit=entry["unit"] if isinstance(entry["unit"], str) else None,
            record_count=int(entry["count"]),
            is_clean_constant=bool(entry["clean"]) and bool(entry["set"]),
            quality_summary=", ".join(sorted(entry["qualities"])),
        )
        for signal_id, entry in state.items()
    }
