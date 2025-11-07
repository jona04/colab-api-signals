from abc import ABC, abstractmethod
from typing import Dict


class IndicatorRepository(ABC):
    """
    Repository interface for persisting technical indicator snapshots per candle.
    """

    @abstractmethod
    async def ensure_indexes(self) -> None:
        """
        Ensure indexes for indicator collection (e.g., symbol+ts uniqueness).
        """
        raise NotImplementedError

    @abstractmethod
    async def upsert_snapshot(self, snapshot: Dict) -> None:
        """
        Upsert a per-candle indicator snapshot document.

        Expected unique key: (symbol, ts).
        """
        raise NotImplementedError
