import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, Field, field_validator

from core.usecases.evaluate_active_strategies_use_case import EvaluateActiveStrategiesUseCase
from core.services.strategy_reconciler_service import StrategyReconcilerService
from adapters.external.database.strategy_repository_mongodb import StrategyRepositoryMongoDB
from adapters.external.database.strategy_episode_repository_mongodb import StrategyEpisodeRepositoryMongoDB
from adapters.external.database.signal_repository_mongodb import SignalRepositoryMongoDB
from adapters.external.database.trigger_event_repository_mongodb import TriggerEventRepositoryMongoDB
from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient
from adapters.external.market_data.market_data_http_client import MarketDataHttpClient

from .deps import get_db


router = APIRouter(prefix="/triggers", tags=["triggers"])


class CandleClosedTriggerDTO(BaseModel):
    indicator_set_id: str = Field(..., description="Indicator set logical id (cfg_hash).")
    ts: int = Field(..., description="Closed candle timestamp in ms.")
    indicator_set: Optional[Dict[str, Any]] = Field(
        None, description="Optional full indicator set document."
    )
    indicator_snapshot: Optional[Dict[str, Any]] = Field(
        None, description="Optional full indicator snapshot for the closed candle."
    )

    @field_validator("indicator_set_id")
    @classmethod
    def _strip_id(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("indicator_set_id is required")
        return v

    @field_validator("ts")
    @classmethod
    def _valid_ts(cls, v: int) -> int:
        v = int(v)
        if v <= 0:
            raise ValueError("ts must be a positive integer (ms)")
        return v


@router.post("/candle-closed")
async def candle_closed_trigger(
    dto: CandleClosedTriggerDTO,
    request: Request,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> Dict[str, Any]:
    logger = logging.getLogger("CandleClosedTrigger")

    # 1) Idempotency guard
    trig_repo = TriggerEventRepositoryMongoDB(db)
    is_new = await trig_repo.mark_if_new(dto.indicator_set_id, dto.ts)
    if not is_new:
        return {"ok": True, "processed": False, "reason": "duplicate_event"}

    indicator_set = dto.indicator_set
    indicator_snapshot = dto.indicator_snapshot

    # 2) If missing, fetch from api-market-data
    market_data_base_url: str = getattr(request.app.state, "market_data_base_url", "") or ""
    md_client: Optional[MarketDataHttpClient] = (
        MarketDataHttpClient(base_url=market_data_base_url) if market_data_base_url else None
    )

    if indicator_set is None:
        if not md_client:
            raise HTTPException(
                status_code=400,
                detail="indicator_set not provided and MARKET_DATA_BASE_URL is not configured",
            )
        indicator_set = await md_client.get_indicator_set(dto.indicator_set_id)
        if not indicator_set:
            raise HTTPException(status_code=404, detail="indicator_set not found in market-data")

    if indicator_snapshot is None:
        if not md_client:
            raise HTTPException(
                status_code=400,
                detail="indicator_snapshot not provided and MARKET_DATA_BASE_URL is not configured",
            )
        indicator_snapshot = await md_client.get_indicator_snapshot(dto.indicator_set_id, dto.ts)
        if not indicator_snapshot:
            raise HTTPException(status_code=404, detail="indicator_snapshot not found in market-data")

    # Basic validation that UC expects
    required = ["symbol", "close", "ema_fast", "ema_slow", "atr_pct", "ts"]
    for k in required:
        if k not in (indicator_snapshot or {}):
            raise HTTPException(status_code=400, detail=f"indicator_snapshot missing required field: {k}")

    # 3) Build and run use case
    strategy_repo = StrategyRepositoryMongoDB(db)
    episode_repo = StrategyEpisodeRepositoryMongoDB(db)
    signal_repo = SignalRepositoryMongoDB(db)

    pipeline_base_url: str = getattr(request.app.state, "pipeline_base_url", "") or ""
    lp_client = PipelineHttpClient(base_url=pipeline_base_url) if pipeline_base_url else PipelineHttpClient(base_url="")

    reconciler = StrategyReconcilerService(lp_client=lp_client)

    uc = EvaluateActiveStrategiesUseCase(
        strategy_repo=strategy_repo,
        episode_repo=episode_repo,
        signal_repo=signal_repo,
        reconciling_service=reconciler,
        lp_client=lp_client,
        logger=logger,
    )

    await uc.execute_for_indicator_snapshot(
        indicator_set=indicator_set,
        indicator_snapshot=indicator_snapshot,
    )

    return {
        "ok": True,
        "processed": True,
        "indicator_set_id": dto.indicator_set_id,
        "ts": dto.ts,
        "symbol": indicator_snapshot.get("symbol"),
    }
