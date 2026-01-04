from typing import Dict, List, Literal, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator
from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.strategy_entity import StrategyEntity

from .deps import get_db
from ...external.database.strategy_repository_mongodb import StrategyRepositoryMongoDB

router = APIRouter(prefix="/admin", tags=["admin"])

# =========================
# Strategies (POST)
# =========================

class RangeTierDTO(BaseModel):
    name: str
    atr_pct_threshold: float = Field(..., ge=0.0)
    atr_pct_threshold_down: Optional[float] = Field(
        None,
        ge=0.0,
        description="ATR limite em tendência de baixa (se None, usa atr_pct_threshold padrão)"
    )
    bars_required: int = Field(..., ge=1)
    max_major_side_pct: float = Field(..., gt=0.0)
    allowed_from: List[str] = Field(default_factory=list)

class StrategyParamsDTO(BaseModel):
    # trend / skew
    skew_low_pct: float = Field(0.09, ge=0.0)
    skew_high_pct: float = Field(0.01, ge=0.0)

    # global caps/floors
    max_major_side_pct: Optional[float] = Field(None, ge=0.0)
    vol_high_threshold_pct: Optional[float] = Field(0.02, ge=0.0)
    vol_high_threshold_pct_down: Optional[float] = Field(
        None,
        ge=0.0,
        description="ATR limite para considerar alta volatilidade em tendência de baixa"
    )
    
    # pool caps
    high_vol_max_major_side_pct: float = Field(0.10, ge=0.0)
    standard_max_major_side_pct: float = Field(0.05, ge=0.0)

    # tiers (narrowest wins)
    tiers: List[RangeTierDTO] = Field(default_factory=list)

    # operation/cooldowns
    eps: float = Field(1e-6, ge=0.0)
    cooloff_bars: int = Field(1, ge=0)

    inrange_resize_mode: Literal["preserve", "skew_swap"] = Field("skew_swap")
    breakout_confirm_bars: int = Field(1, ge=1)
    
    # ===== integração vault on-chain =====
    dex: Optional[str] = Field(None, description="ex: 'aerodrome', 'uniswap'")
    alias: Optional[str] = Field(None, description="vault alias usado nas rotas /vaults/{dex}/{alias}")
    token0_address: Optional[str] = Field(None, description="address token0 do par/vault")
    token1_address: Optional[str] = Field(None, description="address token1 do par/vault")
    
    # ===== gauge / staking =====
    gauge_flow_enabled: bool = Field(False, description="Se true, usa fluxo UNSTAKE→WITHDRAW→SWAP→OPEN→STAKE")
    
class StrategyCreateDTO(BaseModel):
    name: str = Field(..., examples=["eth_range_v1"])
    symbol: str = Field(..., examples=["ETHUSDT"])
    indicator_set_id: str = Field(..., description="Use the indicator set cfg_hash")
    params: StrategyParamsDTO

    @field_validator("symbol")
    @classmethod
    def upper_symbol(cls, v: str) -> str:
        return v.upper()

class StrategyOutDTO(BaseModel):
    name: str
    symbol: str
    status: str
    indicator_set_id: Optional[str] = None
    params: Optional[StrategyParamsDTO] = None
    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None
    updated_at: Optional[int] = None

@router.post("/strategies", response_model=StrategyOutDTO)
async def create_strategy(dto: StrategyCreateDTO, db: AsyncIOMotorDatabase = Depends(get_db)):
    """
    Create or upsert a Strategy linked to an indicator set.
    'indicator_set_id' must be the cfg_hash returned by /indicator-sets.
    """
    # Store strategy with indicator_set_id and cfg_hash (same logical id here)
    strat_repo = StrategyRepositoryMongoDB(db)
    stored = await strat_repo.upsert(
       StrategyEntity(
            name = dto.name,
            symbol = dto.symbol,
            status= "ACTIVE",
            indicator_set_id= dto.indicator_set_id,
            params = dto.params.model_dump(),
       )
    )
    if not stored:
        raise HTTPException(status_code=500, detail="Failed to upsert strategy")
    return stored
