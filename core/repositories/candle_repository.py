from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class CandleRepository(ABC):
    """
    Repository interface for persisting and querying closed candles.
    """

    @abstractmethod
    async def ensure_indexes(self) -> None:
        """
        Ensure the collection has the proper indexes (e.g., unique compound index).
        """
        raise NotImplementedError

    @abstractmethod
    async def upsert_closed_candle(self, candle_doc: Dict) -> None:
        """
        Upsert a closed 1m candle using a unique key (symbol, interval, open_time).

        :param candle_doc: Document containing candle fields already normalized.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_last_n_closed(self, symbol: str, interval: str, n: int) -> List[Dict]:
        """
        Return the last N closed candles for a given symbol and interval, sorted ascending by close_time.

        :param symbol: Trading symbol, e.g. 'ETHUSDT'.
        :param interval: Interval string, e.g. '1m'.
        :param n: Number of candles to return.
        :return: List of candle documents (ascending by close_time).
        """
        raise NotImplementedError

    @abstractmethod
    async def get_last_closed(self, symbol: str, interval: str) -> Optional[Dict]:
        """
        Return the most recent closed candle for the given symbol and interval.

        :param symbol: Trading symbol.
        :param interval: Interval string.
        :return: Candle document or None.
        """
        raise NotImplementedError
