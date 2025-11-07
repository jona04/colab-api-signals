from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class IndicatorSetRepository(ABC):
    """
    Repository interface for managing indicator sets (dedup tuple per symbol).
    """

    @abstractmethod
    async def ensure_indexes(self) -> None:
        """Ensure unique index on (symbol, ema_fast, ema_slow, atr_window)."""
        raise NotImplementedError

    @abstractmethod
    async def upsert_active(self, doc: Dict) -> Dict:
        """
        Upsert an ACTIVE indicator set and return the stored document.
        Expected keys: symbol, ema_fast, ema_slow, atr_window, cfg_hash, status.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_active_by_symbol(self, symbol: str) -> List[Dict]:
        """Return all ACTIVE sets for a symbol."""
        raise NotImplementedError

    @abstractmethod
    async def get_by_id(self, indicator_set_id: str) -> Optional[Dict]:
        """Fetch one indicator set by id."""
        raise NotImplementedError

    @abstractmethod
    async def find_one_by_tuple(self, symbol: str, ema_fast: int, ema_slow: int, atr_window: int) -> Optional[Dict]:
        """Find set by tuple."""
        raise NotImplementedError
