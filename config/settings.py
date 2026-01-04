"""
Application configuration for api-signals.

Centralizes environment variables using python-dotenv.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # MongoDB
    MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    MONGODB_DB_NAME: str = os.getenv("MONGODB_DB_NAME", "signals_db")
    MONGODB_MAX_POOL_SIZE: int = int(os.getenv("MONGODB_MAX_POOL_SIZE", "50"))
    MONGODB_SERVER_SELECTION_TIMEOUT_MS: int = int(
        os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT_MS", "5000")
    )
    MONGODB_CONNECT_TIMEOUT_MS: int = int(os.getenv("MONGODB_CONNECT_TIMEOUT_MS", "3000"))
    MONGODB_SOCKET_TIMEOUT_MS: int = int(os.getenv("MONGODB_SOCKET_TIMEOUT_MS", "10000"))

    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # External APIs
    MARKET_DATA_BASE_URL: str = os.getenv("MARKET_DATA_BASE_URL", "http://172.17.0.1:8081")
    LP_BASE_URL: str = os.getenv("LP_BASE_URL", "http://172.17.0.1:8000")

    # Backfill behavior
    ENABLE_BACKFILL_ON_START: bool = os.getenv("ENABLE_BACKFILL_ON_START", "true").lower() == "true"

    # Log / app
    APP_NAME: str = os.getenv("APP_NAME", "api-signals")


settings = Settings()
