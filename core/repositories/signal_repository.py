from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class SignalRepository(ABC):

    @abstractmethod
    async def ensure_indexes(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def upsert_signal(self, doc: Dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def list_pending(self, limit: int = 50) -> List[Dict]:
        """
        Return latest pending signals to process.
        """
        raise NotImplementedError

    @abstractmethod
    async def mark_success(self, signal: Dict) -> None:
        """
        Mark as SENT (success).
        """
        raise NotImplementedError

    @abstractmethod
    async def mark_failure(self, signal: Dict, error_msg: str) -> None:
        """
        Mark as FAILED with last_error.
        """
        raise NotImplementedError