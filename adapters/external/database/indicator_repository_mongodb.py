import time
from datetime import datetime, timezone
from typing import Dict

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.repositories.indicator_repository import IndicatorRepository


class IndicatorRepositoryMongoDB(IndicatorRepository):
    """
    MongoDB implementation for IndicatorRepository.

    Stores one document per candle with both OHLC fields and indicator outputs.
    """

    COLLECTION_NAME = "indicators_1m"

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        :param db: Motor async database instance.
        """
        self._db = db
        self._collection = self._db[self.COLLECTION_NAME]

    async def ensure_indexes(self) -> None:
        """
        Create unique index on (symbol, ts) and read index on (symbol, ts desc).
        """
        await self._collection.create_index(
            [("symbol", 1), ("ts", 1)],
            unique=True,
            name="ux_symbol_ts",
        )
        await self._collection.create_index(
            [("symbol", 1), ("ts", -1)],
            unique=False,
            name="ix_symbol_ts_desc",
        )

    async def upsert_snapshot(self, snapshot: Dict) -> None:
        """
        Upsert snapshot keyed by (symbol, ts).

        Adds updated_at; sets created_at on insert.
        """
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        
        key = {"symbol": snapshot["symbol"], "ts": snapshot["ts"]}
        update = {
            "$set": {**snapshot, "updated_at": now_ms},
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso
                },
        }
        await self._collection.update_one(key, update, upsert=True)
