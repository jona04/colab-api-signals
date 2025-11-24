# core/domain/entities/candle_entity.py
from typing import Optional
from pydantic import ConfigDict
from .base_entity import MongoEntity

class CandleEntity(MongoEntity):
    symbol: str
    interval: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int
    is_closed: bool = True

    model_config = ConfigDict(extra="ignore")
