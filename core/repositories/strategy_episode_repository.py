from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List


class StrategyEpisodeRepository(ABC):
    """
    Repository interface for managing open/closed episodes (active bands) per strategy.
    """

    @abstractmethod
    async def ensure_indexes(self) -> None:
        """Indexes for (strategy_id,status) and time-ordered queries."""
        raise NotImplementedError

    @abstractmethod
    async def get_open_by_strategy(self, strategy_id: str) -> Optional[Dict]:
        """Return the OPEN episode for a strategy or None."""
        raise NotImplementedError

    @abstractmethod
    async def open_new(self, doc: Dict) -> Dict:
        """Insert a new OPEN episode; returns the stored document."""
        raise NotImplementedError

    @abstractmethod
    async def close_episode(self, episode_id: str, close_fields: Dict) -> None:
        """Mark an episode CLOSED with provided fields (reason, times, metrics)."""
        raise NotImplementedError

    @abstractmethod
    async def update_partial(self, episode_id: str, partial: Dict) -> None:
        """Patch fields on the open episode (e.g., streaks, last_event_bar)."""
        raise NotImplementedError

    @abstractmethod
    async def list_by_strategy(self, strategy_id: str, limit: int = 50) -> List[Dict]:
        """History: recent episodes for a strategy."""
        raise NotImplementedError

    @abstractmethod
    async def append_execution_log(
        self,
        episode_id: str,
        log: Dict[str, Any],
    ) -> None:
        raise NotImplementedError