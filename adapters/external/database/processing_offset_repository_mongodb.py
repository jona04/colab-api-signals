import time
from datetime import datetime, timezone
from typing import Optional, Dict

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.repositories.processing_offset_repository import ProcessingOffsetRepository


class ProcessingOffsetRepositoryMongoDB(ProcessingOffsetRepository):
    """
    MongoDB implementation for processing offsets.
    """

    COLLECTION_NAME = "processing_offsets"

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        :param db: Motor async database instance.
        """
        self._db = db
        self._collection = self._db[self.COLLECTION_NAME]

    async def ensure_indexes(self) -> None:
        """
        Unique index on 'stream'.
        """
        await self._collection.create_index(
            [("stream", 1)],
            unique=True,
            name="ux_stream",
        )

    async def set_last_closed_open_time(self, stream: str, open_time_ms: int) -> None:
        """
        Upsert last closed candle open time and update last_sync_at.
        """
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        
        key = {"stream": stream}
        update = {
            "$set": {
                "last_closed_open_time": open_time_ms,
                "last_sync_at": now_ms,
            },
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso,
                },
        }
        await self._collection.update_one(key, update, upsert=True)

    async def get_by_stream(self, stream: str) -> Optional[Dict]:
        """
        Get offset document by stream key.
        """
        return await self._collection.find_one({"stream": stream})
