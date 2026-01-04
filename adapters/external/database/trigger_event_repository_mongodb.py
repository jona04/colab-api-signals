import time
from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.repositories.trigger_event_repository import TriggerEventRepository


class TriggerEventRepositoryMongoDB(TriggerEventRepository):
    """
    MongoDB implementation for trigger-event idempotency.

    Collection stores one document per processed (indicator_set_id, ts) pair.
    """

    COLLECTION = "trigger_events"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        """
        Ensure a unique index on (indicator_set_id, ts) to guarantee idempotency.
        """
        await self._col.create_index(
            [("indicator_set_id", 1), ("ts", 1)],
            unique=True,
            name="ux_indicator_set_ts",
        )
        await self._col.create_index(
            [("created_at", -1)],
            name="ix_created_at",
        )

    async def mark_if_new(self, indicator_set_id: str, ts: int) -> bool:
        """
        Atomically inserts the marker if missing.

        Returns:
            True if inserted (new event),
            False if already exists.
        """
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

        key = {"indicator_set_id": indicator_set_id, "ts": int(ts)}
        doc = {**key, "created_at": now_ms, "created_at_iso": now_iso}

        try:
            await self._col.insert_one(doc)
            return True
        except Exception:
            # Duplicate key = already processed.
            # We keep it broad because motor raises different exception classes depending on environment.
            return False
