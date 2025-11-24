import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.candle_entity import CandleEntity
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

    async def upsert_closed_candle(self, candle: CandleEntity) -> None:
        """
        Upsert the closed candle. Uses (symbol, interval, open_time) as the unique key.

        Adds/refreshes updated_at; sets created_at on insert.
        """
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        
        doc = candle.to_mongo()
        key = {
            "symbol": candle.symbol,
            "interval": candle.interval,
            "open_time": candle.open_time,
        }
        update = {
            "$set": {
                **doc,
                "is_closed": True,
                "updated_at": now_ms,
            },
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso,
            },
        }
        await self._collection.update_one(key, update, upsert=True)

    async def get_last_n_closed(self, symbol: str, interval: str, n: int) -> List[CandleEntity]:
        """
        Return the last N closed candles sorted ascending by close_time.
        """
        cursor = self._collection.find(
            {"symbol": symbol, "interval": interval, "is_closed": True},
            sort=[("close_time", -1)],
            limit=max(1, n),
        )
        docs = await cursor.to_list(length=max(1, n))
        docs.reverse()
        return [CandleEntity.from_mongo(d) for d in docs if d]

    async def get_last_closed(self, symbol: str, interval: str) -> Optional[CandleEntity]:
        """
        Return the most recent closed candle for the given symbol and interval.
        """
        doc = await self._collection.find_one(
            {"symbol": symbol, "interval": interval, "is_closed": True},
            sort=[("close_time", -1)],
            projection={"_id": False}
        )
        return CandleEntity.from_mongo(doc)
