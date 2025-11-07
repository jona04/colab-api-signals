import time
from datetime import datetime, timezone
from hashlib import sha1
from typing import Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.repositories.indicator_set_repository import IndicatorSetRepository


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

    async def upsert_active(self, doc: Dict) -> Dict:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
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
        return await self._col.find_one(key, projection={"_id": False})

    async def get_active_by_symbol(self, symbol: str) -> List[Dict]:
        cursor = self._col.find({"symbol": symbol, "status": "ACTIVE"}, projection={"_id": False})
        return await cursor.to_list(length=None)

    async def get_by_id(self, indicator_set_id: str) -> Optional[Dict]:
        return await self._col.find_one({"_id": indicator_set_id}, projection={"_id": False})

    async def find_one_by_tuple(self, symbol: str, ema_fast: int, ema_slow: int, atr_window: int) -> Optional[Dict]:
        return await self._col.find_one(
            {"symbol": symbol, "ema_fast": ema_fast, "ema_slow": ema_slow, "atr_window": atr_window},
            projection={"_id": False},
        )
