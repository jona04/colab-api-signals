import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.repositories.strategy_episode_repository import StrategyEpisodeRepository


class StrategyEpisodeRepositoryMongoDB(StrategyEpisodeRepository):
    """
    Mongo implementation for strategy episodes (bands).
    """

    COLLECTION = "strategy_episodes"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        await self._col.create_index([("strategy_id", 1), ("status", 1)], name="ix_strategy_status")
        await self._col.create_index([("strategy_id", 1), ("open_time", -1)], name="ix_strategy_open_time")

    async def get_open_by_strategy(self, strategy_id: str) -> Optional[Dict]:
        return await self._col.find_one(
            {"strategy_id": strategy_id, "status": "OPEN"},
        )

    async def open_new(self, doc: Dict) -> Dict:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        payload = {
            **doc,
            "status": "OPEN",
            "created_at": now_ms,
            "created_at_iso": now_iso,
            "updated_at": now_ms,
            "out_above_streak": doc.get("out_above_streak", 0),
            "out_below_streak": doc.get("out_below_streak", 0),
        }
        await self._col.insert_one(payload)
        return payload

    async def close_episode(self, episode_id: str, close_fields: Dict) -> None:
        now_ms = int(time.time() * 1000)
        await self._col.update_one(
            {"_id": episode_id},
            {"$set": {**close_fields, "status": "CLOSED", "updated_at": now_ms}},
        )

    async def update_partial(self, episode_id: str, partial: Dict) -> None:
        now_ms = int(time.time() * 1000)
        await self._col.update_one({"_id": episode_id}, {"$set": {**partial, "updated_at": now_ms}})

    async def list_by_strategy(self, strategy_id: str, limit: int = 50) -> List[Dict]:
        cursor = self._col.find({"strategy_id": strategy_id}, sort=[("open_time", -1)], limit=limit, projection={"_id": False})
        return await cursor.to_list(length=limit)

    async def append_execution_log(
        self,
        episode_id: str,
        log: Dict[str, Any],
    ) -> None:
        """
        Pushes a new execution log entry to the episode.execution_log array.
        Adds timestamps automatically if missing.
        """
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

        final_log = {
            "ts": now_ms,
            "ts_iso": now_iso,
            **log,
        }

        await self._col.update_one(
            {"_id": episode_id},
            {"$push": {"execution_log": final_log}},
        )