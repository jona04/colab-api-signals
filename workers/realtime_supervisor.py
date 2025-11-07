import asyncio
import contextlib
import logging
import os

from motor.motor_asyncio import AsyncIOMotorClient

from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient
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

        self._ws_client: BinanceWebsocketClient | None = None
        self._ingestion_use_case: StartRealtimeIngestionUseCase | None = None

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
        mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        mongodb_db_name = os.getenv("MONGODB_DB_NAME", "signals_db")
        symbol = os.getenv("BINANCE_STREAM_SYMBOL", "ethusdt")
        interval = os.getenv("BINANCE_STREAM_INTERVAL", "1m")

        # Mongo
        self._mongo_client = AsyncIOMotorClient(mongodb_uri)
        self._db = self._mongo_client[mongodb_db_name]

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

        # WebSocket client
        self._ws_client = BinanceWebsocketClient()

        # Strategy infra
        indicator_set_repo = IndicatorSetRepositoryMongoDB(self._db)
        strategy_repo = StrategyRepositoryMongoDB(self._db)
        episode_repo = StrategyEpisodeRepositoryMongoDB(self._db)
        signal_repo = SignalRepositoryMongoDB(self._db)
        
        await indicator_set_repo.ensure_indexes()
        await strategy_repo.ensure_indexes()
        await episode_repo.ensure_indexes()
        await signal_repo.ensure_indexes()

        # pipeline/vault HTTP client (our LP bridge)
        pipeline_base_url = os.getenv("LP_BASE_URL", "http://172.17.0.1:8000")
        pipeline_http = PipelineHttpClient(pipeline_base_url)

        # Reconciler: turns desired band -> ordered steps [COLLECT, WITHDRAW, SWAP, REBALANCE]
        reconciler = StrategyReconcilerService(pipeline_http)

        # EvaluateActiveStrategiesUseCase: called whenever we compute a fresh indicator snapshot
        evaluate_uc = EvaluateActiveStrategiesUseCase(
            strategy_repo=strategy_repo,
            episode_repo=episode_repo,
            signal_repo=signal_repo,
            reconciling_service=reconciler,
        )

        # Realtime ingestion orchestration (candles -> indicators -> evaluate strategies)
        self._ingestion_use_case = StartRealtimeIngestionUseCase(
            symbol=symbol,
            interval=interval,
            websocket_client=self._ws_client,
            candle_repository=candle_repo,
            processing_offset_repository=offset_repo,
            compute_indicators_use_case=compute_indicators_uc,
            indicator_set_repo=indicator_set_repo,
            evaluate_use_case=evaluate_uc,
        )

        # Signal executor: drains PENDING and hits the pipeline HTTP in loop
        self._signal_executor_uc = ExecuteSignalPipelineUseCase(
            signal_repo=signal_repo,
            episode_repo = episode_repo,
            lp_client=pipeline_http,
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

        # spawn executor loop task
        self._executor_task = asyncio.create_task(_executor_loop())

        # finally, start ingestion (this call sets up WS consuming etc.)
        await self._ingestion_use_case.execute()
        self._logger.info("Realtime ingestion started for %s@%s", symbol, interval)


    async def stop(self):
        """
        Gracefully stop resources.
        """
        # stop background executor loop
        if self._executor_task:
            self._executor_task.cancel()
            with contextlib.suppress(Exception):
                await self._executor_task

        # close WS
        if self._ws_client:
            await self._ws_client.close()

        # close Mongo
        if self._mongo_client:
            self._mongo_client.close()
