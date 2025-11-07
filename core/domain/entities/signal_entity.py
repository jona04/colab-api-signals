# apps/api-signals/core/domain/entities/signal_entity.py

from typing import Any, Dict, Optional
from pydantic import BaseModel
from ..enums.signal_enums import SignalStatus, SignalType


class SignalEntity(BaseModel):
    """
    Canonical in-memory representation of a signal document
    saved in the 'signals' collection.

    This mirrors what EvaluateActiveStrategiesUseCase writes
    and what ExecuteSignalPipelineUseCase will later consume.
    """

    strategy_id: str
    indicator_set_id: str
    cfg_hash: str
    symbol: str
    ts: int  # unix-ish timestamp from the snapshot that created this signal

    signal_type: SignalType
    status: SignalStatus = SignalStatus.PENDING
    attempts: int = 0

    # payload is the structured instruction for the executor.
    # We do NOT enforce a strict schema here because it can vary
    # slightly depending on signal_type, but in practice we expect:
    #
    # {
    #   "dex": "aerodrome" | "uniswap" | ...
    #   "alias": "vault1",
    #   "token0_address": "...",
    #   "token1_address": "...",
    #
    #   "rebalance": {
    #       "lower_tick": int,
    #       "upper_tick": int,
    #       "lower_price": float,
    #       "upper_price": float,
    #       "cap0": float,
    #       "cap1": float
    #   },
    #
    #   "swap": {
    #       "token_in": str,
    #       "token_out": str,
    #       "amount_in": float,
    #       "amount_in_usd": float
    #   }
    # }
    payload: Dict[str, Any]

    # Optional execution result metadata
    last_error: Optional[str] = None
