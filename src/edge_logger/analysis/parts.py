"""Physical part totals require declared cavity counts and quantity semantics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..models import ProcessRecord, QualityStatus


@dataclass(slots=True)
class PartTotals:
    produced: int = 0
    rejected: int = 0
    samples: int = 0
    produced_known: bool = True
    rejected_known: bool = True
    samples_known: bool = True

    def add(self, record: ProcessRecord, registry: dict[str, dict[str, Any]]) -> None:
        cavities = _quantity(record.production_context.get("active_cavities"))
        if cavities is None or cavities == 0:
            self.produced_known = False
        else:
            self.produced += cavities
        rejected = _quantity(record.production_context.get("rejected_parts"))
        if rejected is None:
            quality = [
                (signal, registry.get(signal.signal_id, {}))
                for signal in record.signals
                if registry.get(signal.signal_id, {}).get("exception_rule") == "bad_part"
            ]
            if len(quality) == 1 and quality[0][0].quality is QualityStatus.GOOD:
                signal, metadata = quality[0]
                if metadata.get("bad_part_mode", "indicator") == "quantity":
                    rejected = _quantity(signal.value)
                elif cavities == 1 and signal.value in (0, 1, False, True):
                    rejected = int(bool(signal.value))
                elif signal.value in (0, False):
                    rejected = 0
        if rejected is None or (cavities is not None and rejected > cavities):
            self.rejected_known = False
        else:
            self.rejected += rejected
        samples = _quantity(record.production_context.get("sample_parts"))
        if samples is None or (cavities is not None and samples + (rejected or 0) > cavities):
            self.samples_known = False
        else:
            self.samples += samples

    @property
    def good(self) -> int | None:
        if self.produced_known and self.rejected_known and self.samples_known:
            return self.produced - self.rejected - self.samples
        return None


def _quantity(value: object) -> int | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
        try:
            integer = int(value)
            return integer if integer == value else None
        except (ValueError, OverflowError):
            pass
    return None
