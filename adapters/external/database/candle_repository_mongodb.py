import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.repositories.candle_repository import CandleRepository


class CandleRepositoryMongoDB(CandleRepository):
    """
    MongoDB implementation for CandleRepository using Motor.
    """

    COLLECTION_NAME = "candles_1m"

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        :param db: Motor async database instance.
        """
        self._db = db
        self._collection = self._db[self.COLLECTION_NAME]

    async def ensure_indexes(self) -> None:
        """
        Create a unique compound index on (symbol, interval, open_time) for idempotency
        and a non-unique index on (symbol, interval, close_time) for reads.
        """
        await self._collection.create_index(
            [("symbol", 1), ("interval", 1), ("open_time", 1)],
            unique=True,
            name="ux_symbol_interval_open_time",
        )
        await self._collection.create_index(
            [("symbol", 1), ("interval", 1), ("close_time", 1)],
            unique=False,
            name="ix_symbol_interval_close_time",
        )

    async def upsert_closed_candle(self, candle_doc: Dict) -> None:
        """
        Upsert the closed candle. Uses (symbol, interval, open_time) as the unique key.

        Adds/refreshes updated_at; sets created_at on insert.
        """
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        
        key = {
            "symbol": candle_doc["symbol"],
            "interval": candle_doc["interval"],
            "open_time": candle_doc["open_time"],
        }
        update = {
            "$set": {
                "close_time": candle_doc["close_time"],
                "open": candle_doc["open"],
                "high": candle_doc["high"],
                "low": candle_doc["low"],
                "close": candle_doc["close"],
                "volume": candle_doc["volume"],
                "trades": candle_doc["trades"],
                "is_closed": True,
                "updated_at": now_ms,
            },
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso,
                },
        }
        await self._collection.update_one(key, update, upsert=True)

    async def get_last_n_closed(self, symbol: str, interval: str, n: int) -> List[Dict]:
        """
        Return the last N closed candles sorted ascending by close_time.
        """
        cursor = self._collection.find(
            {"symbol": symbol, "interval": interval, "is_closed": True},
            projection={
                "_id": False, "symbol": True, "interval": True,
                "open_time": True, "close_time": True,
                "open": True, "high": True, "low": True, "close": True,
                "volume": True, "trades": True, "is_closed": True
            },
            sort=[("close_time", -1)],
            limit=max(1, n),
        )
        items = await cursor.to_list(length=max(1, n))
        items.reverse()  # ascending
        return items

    async def get_last_closed(self, symbol: str, interval: str) -> Optional[Dict]:
        """
        Return the most recent closed candle for the given symbol and interval.
        """
        doc = await self._collection.find_one(
            {"symbol": symbol, "interval": interval, "is_closed": True},
            sort=[("close_time", -1)],
            projection={"_id": False}
        )
        return doc
