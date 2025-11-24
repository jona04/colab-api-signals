# adapters/external/database/indicator_set_repository_mongodb.py
import time
from datetime import datetime, timezone
from hashlib import sha1
from typing import List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.repositories.indicator_set_repository import IndicatorSetRepository
from core.domain.entities.indicator_set_entity import IndicatorSetEntity


class IndicatorSetRepositoryMongoDB(IndicatorSetRepository):
    """
    Mongo implementation for indicator sets catalog.
    """

    COLLECTION = "indicator_sets"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        await self._col.create_index(
            [("symbol", 1), ("ema_fast", 1), ("ema_slow", 1), ("atr_window", 1)],
            unique=True,
            name="ux_tuple",
        )
        await self._col.create_index([("symbol", 1), ("status", 1)], name="ix_symbol_status")

    async def upsert_active(self, indicator_set: IndicatorSetEntity) -> IndicatorSetEntity:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

        doc = indicator_set.to_mongo()
        if "cfg_hash" not in doc:
            cfg_str = f"{doc['symbol']}|{doc['ema_fast']}|{doc['ema_slow']}|{doc['atr_window']}"
            doc["cfg_hash"] = sha1(cfg_str.encode()).hexdigest()[:16]

        key = {
            "symbol": doc["symbol"],
            "ema_fast": int(doc["ema_fast"]),
            "ema_slow": int(doc["ema_slow"]),
            "atr_window": int(doc["atr_window"]),
        }
        update = {
            "$set": {
                **doc,
                "status": doc.get("status", "ACTIVE"),
                "updated_at": now_ms,
            },
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso,
            },
        }
        await self._col.update_one(key, update, upsert=True)
        found = await self._col.find_one(key)
        return IndicatorSetEntity.from_mongo(found)

    async def get_active_by_symbol(self, symbol: str) -> List[IndicatorSetEntity]:
        cursor = self._col.find({"symbol": symbol, "status": "ACTIVE"})
        docs = await cursor.to_list(length=None)
        return [IndicatorSetEntity.from_mongo(d) for d in docs if d]

    async def get_by_id(self, indicator_set_id: str) -> Optional[IndicatorSetEntity]:
        doc = await self._col.find_one({"_id": indicator_set_id})
        return IndicatorSetEntity.from_mongo(doc)

    async def find_one_by_tuple(
        self, symbol: str, ema_fast: int, ema_slow: int, atr_window: int
    ) -> Optional[IndicatorSetEntity]:
        doc = await self._col.find_one(
            {"symbol": symbol, "ema_fast": ema_fast, "ema_slow": ema_slow, "atr_window": atr_window},
        )
        return IndicatorSetEntity.from_mongo(doc)
