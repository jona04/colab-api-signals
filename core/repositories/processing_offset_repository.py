from abc import ABC, abstractmethod
from typing import Optional, Dict


class ProcessingOffsetRepository(ABC):
    """
    Repository interface for tracking processing offsets/checkpoints
    (e.g., last closed candle open_time).
    """

    @abstractmethod
    async def ensure_indexes(self) -> None:
        """
        Ensure the collection has the proper indexes.
        """
        raise NotImplementedError

    @abstractmethod
    async def set_last_closed_open_time(self, stream: str, open_time_ms: int) -> None:
        """
        Upsert the last closed candle open_time for the given stream key.

        :param stream: Unique stream key (e.g., 'ethusdt_1m').
        :param open_time_ms: Candle open time in milliseconds.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_by_stream(self, stream: str) -> Optional[Dict]:
        """
        Retrieve the processing offset document for a given stream.

        :param stream: Unique stream key.
        :return: Document or None.
        """
        raise NotImplementedError
