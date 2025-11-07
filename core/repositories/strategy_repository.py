from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class StrategyRepository(ABC):
    """
    Repository interface for strategies that reference an indicator set.
    """

    @abstractmethod
    async def ensure_indexes(self) -> None:
        """Indexes for status/symbol/indicator_set_id lookups."""
        raise NotImplementedError

    @abstractmethod
    async def upsert(self, doc: Dict) -> Dict:
        """
        Upsert a strategy by (name, symbol) or by explicit id.
        Must include: name, symbol, status, indicator_set_id, cfg_hash, params{...}
        """
        raise NotImplementedError

    @abstractmethod
    async def get_active_by_indicator_set(self, indicator_set_id: str) -> List[Dict]:
        """Return all ACTIVE strategies for a given indicator_set_id."""
        raise NotImplementedError

    @abstractmethod
    async def get_by_id(self, strategy_id: str) -> Optional[Dict]:
        """Return one strategy by id."""
        raise NotImplementedError
