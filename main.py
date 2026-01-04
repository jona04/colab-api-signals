import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from adapters.entry.http.admin_router import router as admin_router
from adapters.entry.http.trigger_router import router as triggers_router

from adapters.external.database.mongodb_client import get_mongo_client
from config.settings import settings


def _setup_logging() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    _setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting api-signals (lifespan startup)...")

    mongo_client = get_mongo_client()
    db = mongo_client[settings.MONGODB_DB_NAME]

    # Guarda tudo no state (padrão bom p/ microserviços)
    app.state.mongo_client = mongo_client
    app.state.mongo_db = db

    # Base URLs para falar com outras APIs
    app.state.market_data_base_url = settings.MARKET_DATA_BASE_URL
    app.state.pipeline_base_url = settings.LP_BASE_URL

    try:
        # Opcional: valida conexão cedo
        await db.command("ping")
        logger.info("MongoDB ping ok.")
    except Exception:
        logger.exception("MongoDB ping failed (startup).")
        raise

    try:
        yield
    finally:
        logger.info("Shutting down api-signals (lifespan shutdown)...")
        mongo_client.close()
        logger.info("MongoDB client closed.")


app = FastAPI(title="api-signals", version="0.1.0", lifespan=lifespan)

# Routers devem ser incluídos fora do lifespan
app.include_router(admin_router)
app.include_router(triggers_router)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
