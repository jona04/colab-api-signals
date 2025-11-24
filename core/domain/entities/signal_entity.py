# core/domain/entities/signal_entity.py
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict
from ..enums.signal_enums import SignalStatus, SignalType
from .base_entity import MongoEntity
from .strategy_episode_entity import StrategyEpisodeEntity


class SignalStep(BaseModel):
    action: str
    payload: Dict[str, Any] = {}

    model_config = ConfigDict(extra="ignore")


class SignalEntity(MongoEntity):
    """
    Canonical in-memory representation of a signal document
    saved in the 'signals' collection.
    """

    strategy_id: str
    indicator_set_id: str
    cfg_hash: str
    symbol: str
    ts: int  # unix-ish timestamp from the snapshot that created this signal

    signal_type: SignalType
    status: SignalStatus = SignalStatus.PENDING
    attempts: int = 0

    steps: List[SignalStep]

    episode: StrategyEpisodeEntity
    last_episode: Optional[StrategyEpisodeEntity] = None

    last_error: Optional[str] = None

    model_config = ConfigDict(
        use_enum_values=True,
        extra="ignore",
    )
