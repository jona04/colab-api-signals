# core/domain/entities/indicator_set_entity.py
from typing import Optional
from pydantic import ConfigDict
from .base_entity import MongoEntity

class IndicatorSetEntity(MongoEntity):
    symbol: str
    ema_fast: int
    ema_slow: int
    atr_window: int
    cfg_hash: str
    status: str = "ACTIVE"

    model_config = ConfigDict(extra="ignore")
