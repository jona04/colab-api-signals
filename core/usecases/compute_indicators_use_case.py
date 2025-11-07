import logging
from typing import Optional

from ..repositories.candle_repository import CandleRepository
from ..repositories.indicator_repository import IndicatorRepository
from ..services.indicator_calculation_service import IndicatorCalculationService


class ComputeIndicatorsUseCase:
    """
    Use case that, on each closed candle, loads the last required bars and computes
    EMA fast/slow and ATR% for the last candle; then upserts an indicator snapshot.
    """

    def __init__(
        self,
        candle_repository: CandleRepository,
        indicator_repository: IndicatorRepository,
        indicator_service: IndicatorCalculationService,
        logger: logging.Logger | None = None,
    ):
        """
        :param candle_repository: Candle repository.
        :param indicator_repository: Indicator repository.
        :param indicator_service: Indicator calculation helper.
        :param logger: Optional logger.
        """
        self._candle_repo = candle_repository
        self._indicator_repo = indicator_repository
        self._svc = indicator_service
        self._logger = logger or logging.getLogger(self.__class__.__name__)

    @staticmethod
    def required_bars_for(ema_slow: int, atr_window: int) -> int:
        """
        Return the number of bars required to compute all indicators reliably
        for the provided indicator set.
        """
        return max(int(ema_slow), int(atr_window))

    async def execute_for_indicator_set(
        self,
        *,
        symbol: str,
        interval: str,
        ema_fast: int,
        ema_slow: int,
        atr_window: int,
        indicator_set_id: str,
        cfg_hash: str,
    ) -> Optional[dict]:
        """
        Load the last required candles (based on ema_slow/atr_window), compute indicators
        for the last bar and persist the snapshot keyed by (symbol, ts, cfg_hash).

        :param symbol: Trading symbol, e.g. 'ETHUSDT'.
        :param interval: Interval string, e.g. '1m'.
        :param ema_fast: EMA fast period (bars).
        :param ema_slow: EMA slow period (bars).
        :param atr_window: ATR window (bars).
        :param indicator_set_id: Logical id of the set (can be cfg_hash if you prefer).
        :param cfg_hash: Hash representing (symbol, ema_fast, ema_slow, atr_window).
        :return: The snapshot dict persisted, or None if not enough data.
        """
        need = self.required_bars_for(ema_slow, atr_window)
        candles = await self._candle_repo.get_last_n_closed(symbol, interval, need)

        snapshot = self._svc.compute_snapshot_for_last(
            candles,
            ema_fast=int(ema_fast),
            ema_slow=int(ema_slow),
            atr_window=int(atr_window),
        )
        if snapshot is None:
            self._logger.debug("Not enough candles for indicators: have=%s need=%s", len(candles), need)
            return None

        # Enrich with indicator set identifiers (used by indicators_1m unique index)
        snapshot["indicator_set_id"] = indicator_set_id
        snapshot["cfg_hash"] = cfg_hash

        await self._indicator_repo.upsert_snapshot(snapshot)
        self._logger.debug(
            "Indicator snapshot upserted: symbol=%s ts=%s set=%s ema_f=%s ema_s=%s atr=%.6f",
            snapshot["symbol"], snapshot["ts"], cfg_hash,
            snapshot["ema_fast"], snapshot["ema_slow"], snapshot["atr_pct"]
        )
        return snapshot