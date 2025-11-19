import asyncio
import contextlib
import logging
import os

from motor.motor_asyncio import AsyncIOMotorClient

from adapters.external.binance.binance_rest_client import BinanceRestClient
from adapters.external.database.mongodb_client import get_mongo_client
from adapters.external.notify.telegram_notifier import TelegramNotifier
from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient
from config.settings import settings
from core.usecases.backfill_candles_use_case import BackfillCandlesUseCase
from core.usecases.execute_signal_pipeline_use_case import ExecuteSignalPipelineUseCase

from core.services.strategy_reconciler_service import StrategyReconcilerService
from core.usecases.evaluate_active_strategies_use_case import EvaluateActiveStrategiesUseCase

from adapters.external.binance.binance_websocket_client import BinanceWebsocketClient
from adapters.external.database.candle_repository_mongodb import CandleRepositoryMongoDB
from adapters.external.database.processing_offset_repository_mongodb import ProcessingOffsetRepositoryMongoDB
from adapters.external.database.indicator_repository_mongodb import IndicatorRepositoryMongoDB
from adapters.external.database.indicator_set_repository_mongodb import IndicatorSetRepositoryMongoDB
from adapters.external.database.strategy_repository_mongodb import StrategyRepositoryMongoDB
from adapters.external.database.strategy_episode_repository_mongodb import StrategyEpisodeRepositoryMongoDB
from adapters.external.database.signal_repository_mongodb import SignalRepositoryMongoDB
from core.services.indicator_calculation_service import IndicatorCalculationService
from core.usecases.compute_indicators_use_case import ComputeIndicatorsUseCase
from core.usecases.start_realtime_ingestion_use_case import StartRealtimeIngestionUseCase


class RealtimeSupervisor:
    """
    High-level supervisor for the api-signals process.

    Responsibilities:
    - Connect to Mongo, ensure indexes.
    - Wire repositories, services, and use cases.
    - Start realtime ingestion (Binance WS -> candles -> indicators -> strategies -> signals PENDING).
    - Start background executor loop that drains PENDING signals and calls the vault pipeline
      (collect -> withdraw -> swap -> rebalance).
    """

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._mongo_client: AsyncIOMotorClient | None = None
        self._db = None

        self._ws_clients: list[BinanceWebsocketClient] = []
        self._ingestions: list[StartRealtimeIngestionUseCase] = []

        self._signal_executor_uc: ExecuteSignalPipelineUseCase | None = None
        self._executor_task: asyncio.Task | None = None

    @property
    def db(self):
        """Expose the AsyncIOMotorDatabase instance after start()."""
        return self._db
    
    async def start(self):
        """
        Create connections, ensure indexes, and start realtime ingestion.
        """
        # Mongo client centralizado
        self._mongo_client = get_mongo_client()
        self._db = self._mongo_client[settings.MONGODB_DB_NAME]

        # --- Core repos for candles / indicators
        candle_repo = CandleRepositoryMongoDB(self._db)
        offset_repo = ProcessingOffsetRepositoryMongoDB(self._db)
        indicator_repo = IndicatorRepositoryMongoDB(self._db)

        await candle_repo.ensure_indexes()
        await offset_repo.ensure_indexes()
        await indicator_repo.ensure_indexes()

        # Indicator service + use case (stateless periods; provided per call)
        indicator_svc = IndicatorCalculationService()
        compute_indicators_uc = ComputeIndicatorsUseCase(
            candle_repository=candle_repo,
            indicator_repository=indicator_repo,
            indicator_service=indicator_svc,
        )

        # Strategy infra
        indicator_set_repo = IndicatorSetRepositoryMongoDB(self._db)
        strategy_repo = StrategyRepositoryMongoDB(self._db)
        episode_repo = StrategyEpisodeRepositoryMongoDB(self._db)
        signal_repo = SignalRepositoryMongoDB(self._db)
        
        await indicator_set_repo.ensure_indexes()
        await strategy_repo.ensure_indexes()
        await episode_repo.ensure_indexes()
        await signal_repo.ensure_indexes()

        telegram_notifier = None
        if settings.TELEGRAM_BOT_TOKEN and settings.TELEGRAM_CHAT_ID:
            telegram_notifier = TelegramNotifier(
                bot_token=settings.TELEGRAM_BOT_TOKEN,
                chat_id=settings.TELEGRAM_CHAT_ID,
            )
            
        # Binance REST client for backfill
        binance_rest = BinanceRestClient(base_url=settings.BINANCE_REST_BASE_URL)
        backfill_uc = BackfillCandlesUseCase(
            binance_client=binance_rest,
            candle_repository=candle_repo,
            processing_offset_repository=offset_repo,
        )
        
        # pipeline/vault HTTP client (our LP bridge)
        pipeline_http = PipelineHttpClient(settings.LP_BASE_URL)

        # Reconciler: turns desired band -> ordered steps [COLLECT, WITHDRAW, SWAP, REBALANCE]
        reconciler = StrategyReconcilerService(pipeline_http)

        # EvaluateActiveStrategiesUseCase: called whenever we compute a fresh indicator snapshot
        evaluate_uc = EvaluateActiveStrategiesUseCase(
            strategy_repo=strategy_repo,
            episode_repo=episode_repo,
            signal_repo=signal_repo,
            reconciling_service=reconciler,
        )

        # Signal executor use case (background loop)
        self._signal_executor_uc = ExecuteSignalPipelineUseCase(
            signal_repo=signal_repo,
            episode_repo=episode_repo,
            lp_client=pipeline_http,
            notifier=telegram_notifier
        )
        
        async def _executor_loop():
            """
            Forever-loop for executing pending signals. This runs in the background.
            """
            while True:
                try:
                    await self._signal_executor_uc.execute_once()
                except Exception as exc:
                    self._logger.exception("signal executor loop error: %s", exc)
                await asyncio.sleep(5)
        
        self._executor_task = asyncio.create_task(_executor_loop())

        # --- Realtime ingestion for all configured symbols
        interval = settings.BINANCE_STREAM_INTERVAL
        symbols = settings.BINANCE_STREAM_SYMBOLS
        
        if not symbols:
            self._logger.error(
                "No BINANCE_STREAM_SYMBOLS configured. Aborting realtime supervisor start."
            )
            return

        self._logger.info(
            "Initializing realtime ingestion for symbols=%s interval=%s",
            symbols,
            interval,
        )
        
        # Backfill (optional) + start WS for each symbol
        for symbol in symbols:
            symbol = symbol.strip()
            if not symbol:
                continue

            if settings.ENABLE_BACKFILL_ON_START:
                try:
                    await backfill_uc.execute_for_symbol(symbol, interval)
                except Exception as exc:  # noqa: BLE001
                    self._logger.exception(
                        "Backfill error for %s@%s: %s", symbol, interval, exc
                    )

            ws_client = BinanceWebsocketClient(
                base_ws_url=settings.BINANCE_WS_BASE_URL
            )
            ingestion_uc = StartRealtimeIngestionUseCase(
                symbol=symbol,
                interval=interval,
                websocket_client=ws_client,
                candle_repository=candle_repo,
                processing_offset_repository=offset_repo,
                compute_indicators_use_case=compute_indicators_uc,
                indicator_set_repo=indicator_set_repo,
                evaluate_use_case=evaluate_uc,
            )

            self._ws_clients.append(ws_client)
            self._ingestions.append(ingestion_uc)

        # Start all websocket subscriptions
        for ingestion_uc in self._ingestions:
            await ingestion_uc.execute()

        self._logger.info(
            "Realtime ingestion started for symbols=%s@%s",
            symbols,
            interval,
        )


    async def stop(self):
        """
        Gracefully stop background tasks, websockets, and DB connections.
        """
        # stop executor loop
        if self._executor_task:
            self._executor_task.cancel()
            with contextlib.suppress(Exception):
                await self._executor_task

        # close WS clients
        for ws in self._ws_clients:
            with contextlib.suppress(Exception):
                await ws.close()

        # close Mongo
        if self._mongo_client:
            self._mongo_client.close()