import logging
from typing import Any, Dict, List, Optional

from adapters.external.binance.binance_rest_client import BinanceRestClient  # type: ignore
from core.domain.entities.candle_entity import CandleEntity
from core.repositories.candle_repository import CandleRepository
from core.repositories.processing_offset_repository import ProcessingOffsetRepository


class BackfillCandlesUseCase:
    """
    Use case responsible for backfilling missing closed candles using Binance REST API.

    Logic:
      - Reads the last_closed_open_time from processing_offsets for the stream key.
      - If exists, requests klines from (last_closed_open_time + interval_ms) forward.
      - For each returned kline:
          * maps to candle_doc
          * upserts via CandleRepository
          * updates processing_offsets with the new last_closed_open_time
      - Stops when there are no more klines or fewer than the requested limit are returned.

    This guarantees that, after a downtime, we fill the gap BEFORE starting WebSocket ingestion.
    """

    def __init__(
        self,
        binance_client: BinanceRestClient,
        candle_repository: CandleRepository,
        processing_offset_repository: ProcessingOffsetRepository,
        logger: Optional[logging.Logger] = None,
    ):
        self._binance = binance_client
        self._candles = candle_repository
        self._offsets = processing_offset_repository
        self._logger = logger or logging.getLogger(self.__class__.__name__)

    async def execute_for_symbol(self, symbol: str, interval: str) -> None:
        """
        Backfill missing candles for a given symbol/interval based on processing_offsets.

        :param symbol: Trading symbol, e.g. 'ethusdt' (case-insensitive).
        :param interval: Interval string, e.g. '1m'.
        """
        symbol_upper = symbol.upper()
        stream_key = f"{symbol.lower()}_{interval}"

        interval_ms = self._interval_to_ms(interval)
        if interval_ms is None:
            self._logger.warning(
                "Backfill disabled for %s@%s: unsupported interval.",
                symbol_upper,
                interval,
            )
            return

        offset_doc = await self._offsets.get_by_stream(stream_key)
        last_open_time: Optional[int] = None

        if offset_doc and offset_doc.get("last_closed_open_time") is not None:
            last_open_time = int(offset_doc["last_closed_open_time"])

        # If we never stored offset, we don't guess history here.
        # Backfill is strictly "resume from last known candle".
        if last_open_time is None:
            self._logger.info(
                "No existing offset for %s; skipping backfill.", stream_key
            )
            return

        start_time = last_open_time + interval_ms
        if start_time <= 0:
            self._logger.info(
                "Invalid start_time for backfill on %s; skipping.", stream_key
            )
            return

        self._logger.info(
            "Starting backfill for %s@%s from open_time=%s",
            symbol_upper,
            interval,
            start_time,
        )

        # Paginate until no more data (classic Binance 1000 limit loop)
        limit = 1000
        while True:
            klines: List[List[Any]] = await self._binance.get_klines(
                symbol=symbol_upper,
                interval=interval,
                start_time=start_time,
                end_time=None,
                limit=limit,
            )

            if not klines:
                self._logger.info(
                    "Backfill completed for %s@%s (no more klines).",
                    symbol_upper,
                    interval,
                )
                break

            last_batch_open_time = None

            for k in klines:
                # Binance kline format:
                # [0] openTime, [1] open, [2] high, [3] low, [4] close,
                # [5] volume, [6] closeTime, [8] quoteAssetVolume,
                # [8+] ..., [8] numberOfTrades, etc. (varia, mas índices 0-6,8 são estáveis)
                open_time = int(k[0])
                close_time = int(k[6])
                o = float(k[1])
                h = float(k[2])
                l = float(k[3])
                c = float(k[4])
                v = float(k[5])
                trades = int(k[8]) if len(k) > 8 else 0

                candle = CandleEntity(
                    symbol = symbol_upper,
                    interval = interval,
                    open_time= open_time,
                    close_time= close_time,
                    open= o,
                    high= h,
                    low= l,
                    close= c,
                    volume= v,
                    trades= trades,
                    is_closed= True,
                )

                await self._candles.upsert_closed_candle(candle)
                await self._offsets.set_last_closed_open_time(
                    stream_key, open_time
                )
                last_batch_open_time = open_time

            if last_batch_open_time is None:
                self._logger.info(
                    "Backfill completed for %s@%s (no valid klines).",
                    symbol_upper,
                    interval,
                )
                break

            # Prepare next page
            start_time = last_batch_open_time + interval_ms

            # If returned less than limit, likely no more data to fetch now
            if len(klines) < limit:
                self._logger.info(
                    "Backfill finished page for %s@%s (partial batch).",
                    symbol_upper,
                    interval,
                )
                break

        self._logger.info(
            "Backfill executed successfully for %s@%s.", symbol_upper, interval
        )

    @staticmethod
    def _interval_to_ms(interval: str) -> Optional[int]:
        """
        Map simple Binance intervals to milliseconds.
        Currently we only support '1m' as that's the ingestion target.
        """
        mapping = {
            "1m": 60_000,
            # se quiser expandir depois: "5m": 5*60_000, etc.
        }
        return mapping.get(interval)
