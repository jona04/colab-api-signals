# apps/api-signals/adapters/external/database/signal_repository_mongodb.py

import time
from datetime import datetime, timezone
from typing import Dict, List

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.repositories.signal_repository import SignalRepository


class SignalRepositoryMongoDB(SignalRepository):
    """
    Mongo implementation for signal logs (PENDING -> EXECUTED/FAILED).
    """

    COLLECTION = "signals"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        await self._col.create_index(
            [("strategy_id", 1), ("ts", 1), ("signal_type", 1)],
            unique=True,
            name="ux_strategy_ts_type",
        )
        await self._col.create_index(
            [("status", 1), ("created_at", -1)],
            name="ix_status_created_at",
        )

    async def upsert_signal(self, doc: Dict) -> None:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        key = {
            "strategy_id": doc["strategy_id"],
            "ts": doc["ts"],
            "signal_type": doc["signal_type"],
        }
        
        set_doc = {
            k: v
            for (k, v) in doc.items()
            if k not in ("status", "attempts", "created_at", "created_at_iso")
        }
        
        update = {
            "$set": {
                **set_doc,
                "updated_at": now_ms,
            },
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso,
                "status": "PENDING",
                "attempts": 0,
            },
        }
        await self._col.update_one(key, update, upsert=True)

    async def list_pending(self, limit: int = 50) -> List[Dict]:
        cursor = self._col.find(
            {"status": "PENDING"},
            sort=[("created_at", 1)],
            limit=limit,
        )
        docs = await cursor.to_list(length=limit)
        # remove Mongo _id for cleanliness
        for d in docs:
            d.pop("_id", None)
        return docs

    async def mark_success(self, signal: Dict) -> None:
        now_ms = int(time.time() * 1000)
        key = {
            "strategy_id": signal["strategy_id"],
            "ts": signal["ts"],
            "signal_type": signal["signal_type"],
        }
        await self._col.update_one(
            key,
            {"$set": {"status": "SENT", "updated_at": now_ms}},
        )

    async def mark_failure(self, signal: Dict, error_msg: str) -> None:
        now_ms = int(time.time() * 1000)
        key = {
            "strategy_id": signal["strategy_id"],
            "ts": signal["ts"],
            "signal_type": signal["signal_type"],
        }
        await self._col.update_one(
            key,
            {
                "$set": {
                    "status": "FAILED",
                    "updated_at": now_ms,
                    "last_error": error_msg,
                },
                "$inc": {"attempts": 1},
            },
        )