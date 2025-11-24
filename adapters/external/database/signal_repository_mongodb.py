# apps/api-signals/adapters/external/database/signal_repository_mongodb.py

from enum import Enum
import time
from datetime import datetime, timezone
from typing import Dict, List

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.signal_entity import SignalEntity
from core.domain.enums.signal_enums import SignalStatus, SignalType
from core.repositories.signal_repository import SignalRepository


def _norm_signal_type(st) -> str:
    if isinstance(st, Enum):
        return st.value
    return str(st)
    
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

    async def upsert_signal(self, signal: SignalEntity) -> None:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        
        doc = signal.to_mongo()
        key = {
            "strategy_id": signal.strategy_id,
            "ts": signal.ts,
            "signal_type": signal.signal_type.value
            if isinstance(signal.signal_type, SignalType)
            else signal.signal_type,
        }
        
        # não sobreescrever status/attempts em update normal
        set_doc = {
            k: v
            for (k, v) in doc.items()
            if k not in ("status", "attempts", "created_at", "created_at_iso")
        }

        update = {
            "$set": {
                **set_doc,
                "updated_at": now_ms,
                "updated_at_iso": now_iso,
            },
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso,
                "status": SignalStatus.PENDING.value,
                "attempts": 0,
            },
        }
        await self._col.update_one(key, update, upsert=True)

    async def list_pending(self, limit: int = 50) -> List[SignalEntity]:
        cursor = self._col.find(
            {"status": SignalStatus.PENDING.value},
            sort=[("created_at", 1)],
            limit=limit,
        )
        docs = await cursor.to_list(length=limit)
        return [SignalEntity.from_mongo(d) for d in docs if d]

    async def mark_success(self, signal: SignalEntity) -> None:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        st = _norm_signal_type(signal.signal_type)
        key = {
            "strategy_id": signal.strategy_id,
            "ts": signal.ts,
            "signal_type": st,
        }
        await self._col.update_one(
            key,
            {"$set": {"status": SignalStatus.SENT.value, "updated_at": now_ms, "updated_at_iso": now_iso}},
        )


    async def mark_failure(self, signal: SignalEntity, error_msg: str) -> None:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        st = _norm_signal_type(signal.signal_type)
        key = {
            "strategy_id": signal.strategy_id,
            "ts": signal.ts,
            "signal_type": st,
        }
        await self._col.update_one(
            key,
            {
                "$set": {
                    "status": SignalStatus.FAILED.value,
                    "updated_at": now_ms,
                    "updated_at_iso": now_iso,
                    "last_error": error_msg,
                },
                "$inc": {"attempts": 1},
            },
        )