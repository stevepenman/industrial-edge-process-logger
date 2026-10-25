"""Provider contract deliberately limited to read-only record acquisition."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from ..models import ProcessRecord


class AcquisitionProvider(ABC):
    """Provides records without any write or machine-control capability."""

    @abstractmethod
    def records(self) -> AsyncIterator[ProcessRecord]:
        """Yield source records until stopped or the source ends."""
