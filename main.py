import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from adapters.entry.http.admin_router import router as admin_router

from workers.realtime_supervisor import RealtimeSupervisor


def _setup_logging():
    """
    Configure basic logging. You can later replace this with structlog JSON logs.
    """
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


supervisor = RealtimeSupervisor()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context for startup/shutdown lifecycle.
    """
    _setup_logging()
    logging.getLogger(__name__).info("Starting api-signals (lifespan startup)...")
    await supervisor.start()
    
    app.state.db = supervisor.db
    
    app.include_router(admin_router)
    
    try:
        yield
    finally:
        logging.getLogger(__name__).info("Shutting down api-signals (lifespan shutdown)...")
        await supervisor.stop()


app = FastAPI(title="api-signals", version="0.1.0", lifespan=lifespan)

@app.get("/healthz")
async def healthz():
    """
    Liveness probe endpoint.
    """
    return {"status": "ok"}
