import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.strategy_entity import StrategyEntity
from core.repositories.strategy_repository import StrategyRepository


class StrategyRepositoryMongoDB(StrategyRepository):
    """
    Mongo implementation for strategies.
    """

    COLLECTION = "strategies"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        await self._col.create_index([("status", 1), ("symbol", 1)], name="ix_status_symbol")
        await self._col.create_index([("indicator_set_id", 1), ("status", 1)], name="ix_set_status")
        await self._col.create_index([("name", 1), ("symbol", 1)], unique=True, name="ux_name_symbol")

    async def upsert(self, strategy: StrategyEntity) -> StrategyEntity:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

        doc = strategy.to_mongo()
        key = {"name": strategy.name, "symbol": strategy.symbol}
        update = {
            "$set": {
                **doc,
                "updated_at": now_ms,
            },
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso,
            },
        }
        await self._col.update_one(key, update, upsert=True)
        found = await self._col.find_one(key)
        return StrategyEntity.from_mongo(found)

    async def get_active_by_indicator_set(self, indicator_set_id: str) -> List[StrategyEntity]:
        cursor = self._col.find(
            {"indicator_set_id": indicator_set_id, "status": "ACTIVE"},
        )
        docs = await cursor.to_list(length=None)
        return [StrategyEntity.from_mongo(d) for d in docs if d]

    async def get_by_id(self, strategy_id: str) -> Optional[StrategyEntity]:
        doc = await self._col.find_one({"_id": strategy_id})
        return StrategyEntity.from_mongo(doc)
