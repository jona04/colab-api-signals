# core/domain/entities/processing_offset_entity.py
from typing import Optional
from pydantic import ConfigDict
from .base_entity import MongoEntity

class ProcessingOffsetEntity(MongoEntity):
    stream: str
    last_closed_open_time: Optional[int] = None
    last_sync_at: Optional[int] = None

    model_config = ConfigDict(extra="ignore")
