# core/domain/entities/strategy_episode_entity.py
from typing import Any, Dict, List, Optional
from pydantic import ConfigDict
from .base_entity import MongoEntity

class StrategyEpisodeEntity(MongoEntity):
    strategy_id: str
    symbol: str

    pool_type: str = "standard"
    mode_on_open: str
    majority_on_open: str
    target_major_pct: float
    target_minor_pct: float

    open_time: int
    open_time_iso: Optional[str] = None
    open_price: float

    Pa: float
    Pb: float

    last_event_bar: int = 0

    atr_streak: Dict[str, int] = {}
    out_above_streak: int = 0
    out_below_streak: int = 0
    out_above_streak_total: int = 0
    out_below_streak_total: int = 0

    dex: Optional[str] = None
    alias: Optional[str] = None
    token0_address: Optional[str] = None
    token1_address: Optional[str] = None
    gauge_flow_enabled: bool = False

    status: str = "OPEN"
    close_time: Optional[int] = None
    close_time_iso: Optional[str] = None
    close_reason: Optional[str] = None
    close_price: Optional[float] = None

    execution_log: Optional[List[Dict[str, Any]]] = None
    metrics: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="ignore")
