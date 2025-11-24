# core/domain/entities/strategy_entity.py
from typing import Any, Dict, Optional
from pydantic import ConfigDict
from .base_entity import MongoEntity

class StrategyEntity(MongoEntity):
    name: str
    symbol: str
    status: str
    indicator_set_id: str
    params: Dict[str, Any]

    model_config = ConfigDict(extra="ignore")
