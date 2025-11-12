"""
Application configuration for api-signals.

Centralizes environment variables using python-dotenv.
"""

import os
from typing import List

from dotenv import load_dotenv

# Load variables from .env (if present)
load_dotenv()


class Settings:
    """
    Configuration settings for the api-signals service.
    """

    # MongoDB
    MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    MONGODB_DB_NAME: str = os.getenv("MONGODB_DB_NAME", "signals_db")
    MONGODB_MAX_POOL_SIZE: int = int(os.getenv("MONGODB_MAX_POOL_SIZE", "50"))
    MONGODB_SERVER_SELECTION_TIMEOUT_MS: int = int(
        os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT_MS", "5000")
    )
    MONGODB_CONNECT_TIMEOUT_MS: int = int(
        os.getenv("MONGODB_CONNECT_TIMEOUT_MS", "3000")
    )
    MONGODB_SOCKET_TIMEOUT_MS: int = int(
        os.getenv("MONGODB_SOCKET_TIMEOUT_MS", "10000")
    )

    # Binance
    BINANCE_WS_BASE_URL: str = os.getenv(
        "BINANCE_WS_BASE_URL", "wss://stream.binance.com:9443"
    )
    BINANCE_REST_BASE_URL: str = os.getenv(
        "BINANCE_REST_BASE_URL", "https://api.binance.com"
    )

    # Symbols / stream
    # Ex: "ethusdt,btcusdt,solusdt"
    BINANCE_STREAM_SYMBOLS_RAW: str = os.getenv(
        "BINANCE_STREAM_SYMBOLS", os.getenv("BINANCE_STREAM_SYMBOL", "ethusdt")
    )
    BINANCE_STREAM_INTERVAL: str = os.getenv("BINANCE_STREAM_INTERVAL", "1m")

    @property
    def BINANCE_STREAM_SYMBOLS(self) -> List[str]:
        return [
            s.strip()
            for s in self.BINANCE_STREAM_SYMBOLS_RAW.split(",")
            if s.strip()
        ]

    # LP / pipeline HTTP (bridge para vaults / LP strategy)
    LP_BASE_URL: str = os.getenv("LP_BASE_URL", "http://172.17.0.1:8000")

    # Backfill behavior
    # Se True: tenta preencher candles faltantes desde o último offset salvo antes de iniciar o WS
    ENABLE_BACKFILL_ON_START: bool = (
        os.getenv("ENABLE_BACKFILL_ON_START", "true").lower() == "true"
    )

    # Log / app
    APP_NAME: str = os.getenv("APP_NAME", "api-signals")


settings = Settings()
