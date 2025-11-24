from abc import ABC, abstractmethod
from typing import List

from core.domain.entities.signal_entity import SignalEntity


class SignalRepository(ABC):

    @abstractmethod
    async def ensure_indexes(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def upsert_signal(self, signal: SignalEntity) -> None:
        raise NotImplementedError

    @abstractmethod
    async def list_pending(self, limit: int = 50) -> List[SignalEntity]:
        """
        Return latest pending signals to process.
        """
        raise NotImplementedError

    @abstractmethod
    async def mark_success(self, signal: SignalEntity) -> None:
        """
        Mark as SENT (success).
        """
        raise NotImplementedError

    @abstractmethod
    async def mark_failure(self, signal: SignalEntity, error_msg: str) -> None:
        """
        Mark as FAILED with last_error.
        """
        raise NotImplementedError