import logging
from typing import Any, Dict, Optional

from core.domain.entities.candle_entity import CandleEntity

from ..repositories.candle_repository import CandleRepository
from ..repositories.indicator_set_repository import IndicatorSetRepository
from ..usecases.evaluate_active_strategies_use_case import EvaluateActiveStrategiesUseCase
from ..repositories.processing_offset_repository import ProcessingOffsetRepository
from adapters.external.binance.binance_websocket_client import BinanceWebsocketClient  # type: ignore
from .compute_indicators_use_case import ComputeIndicatorsUseCase


class StartRealtimeIngestionUseCase:
    """
    Use case that subscribes to Binance kline_1m websocket for a given symbol and,
    whenever a CLOSED candle arrives, upserts it into MongoDB and updates the processing offset.
    Optionally triggers indicator computation for the symbol.
    """

    def __init__(
        self,
        symbol: str,
        interval: str,
        websocket_client: BinanceWebsocketClient,
        candle_repository: CandleRepository,
        processing_offset_repository: ProcessingOffsetRepository,
        compute_indicators_use_case: Optional[ComputeIndicatorsUseCase] = None,
        indicator_set_repo: Optional[IndicatorSetRepository] = None,
        evaluate_use_case: Optional[EvaluateActiveStrategiesUseCase] = None,
        logger: logging.Logger | None = None,
    ):
        """
        :param symbol: Symbol to subscribe (e.g., 'ethusdt').
        :param interval: Interval string (should be '1m' here).
        :param websocket_client: BinanceWebsocketClient instance.
        :param candle_repository: Candle repository.
        :param processing_offset_repository: Offsets repository.
        :param compute_indicators_use_case: Optional indicators computation use case.
        :param logger: Optional logger.
        """
        self._symbol = symbol.upper()
        self._interval = interval
        self._ws = websocket_client
        self._candle_repo = candle_repository
        self._offset_repo = processing_offset_repository
        self._compute_indicators = compute_indicators_use_case
        self._indicator_set_repo = indicator_set_repo
        self._evaluate_uc = evaluate_use_case
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._stream_key = f"{symbol.lower()}_{self._interval}"
        
    async def execute(self) -> None:
        """
        Start the websocket subscription. This method returns immediately after
        starting the underlying client; the client keeps running in background.
        """
        self._logger.info("Starting realtime ingestion for %s@kline_%s", self._symbol, self._interval)
        await self._ws.subscribe_kline_1m(self._symbol, self._on_kline_closed)

    async def _on_kline_closed(self, event: Dict[str, Any]) -> None:
        """
        Async callback invoked when a CLOSED kline is received.
        Maps the event into our document format and persists it. Then, optionally, computes indicators.
        """
        try:
            k = event["k"]
            candle = CandleEntity(
                symbol=event["s"],
                interval=k["i"],
                open_time=int(k["t"]),
                close_time=int(k["T"]),
                open=float(k["o"]),
                high=float(k["h"]),
                low=float(k["l"]),
                close=float(k["c"]),
                volume=float(k["v"]),
                trades=int(k["n"]),
                is_closed=True,
            )

            await self._candle_repo.upsert_closed_candle(candle)
            await self._offset_repo.set_last_closed_open_time(self._stream_key, candle.open_time)
            self._logger.debug(
                "Upserted candle %s %s open_time=%s",
                candle.symbol, candle.interval, candle.open_time,
            )

            # Trigger indicators and evaluation for each ACTIVE indicator set of this symbol
            if self._compute_indicators is not None and self._indicator_set_repo and self._evaluate_uc:
                try:
                    active_sets = await self._indicator_set_repo.get_active_by_symbol(self._symbol)
                    for indset in active_sets:
                        indicator_snapshot = await self._compute_indicators.execute_for_indicator_set(
                            symbol=self._symbol,
                            interval=self._interval,
                            ema_fast=int(indset.ema_fast),
                            ema_slow=int(indset.ema_slow),
                            atr_window=int(indset.atr_window),
                            indicator_set_id=indset.cfg_hash,
                            cfg_hash=indset.cfg_hash,
                        )
                        if indicator_snapshot:
                            await self._evaluate_uc.execute_for_indicator_snapshot(
                                indicator_set=indset.model_dump(mode="python"),
                                indicator_snapshot=indicator_snapshot,
                            )
                except Exception as exc:
                    self._logger.exception("Failed to compute indicators/evaluate strategies: %s", exc)
        except Exception as exc:
            self._logger.exception("Failed to process closed kline: %s", exc)