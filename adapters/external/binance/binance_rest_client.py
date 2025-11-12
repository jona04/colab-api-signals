import asyncio
import logging
import random
from typing import Any, Dict, List, Optional

import httpx

from config.settings import settings  # type: ignore


class BinanceRestClient:
    """
    Minimal async REST client for Binance public market data.

    Currently supports:
      - GET /api/v3/klines  (used for backfill of historical candles)

    Design:
      - Uses a shared httpx.AsyncClient with sane defaults.
      - Retries on transient errors with exponential backoff + jitter.
      - Does NOT require API key for public endpoints.
    """

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 10.0,
        max_retries: int = 3,
    ) -> None:
        """
        :param base_url: Binance REST base URL (default from settings).
        :param timeout: Request timeout in seconds.
        :param max_retries: Number of retries for transient errors.
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._base_url = (base_url or settings.BINANCE_REST_BASE_URL).rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries

        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def aclose(self) -> None:
        """
        Close the underlying HTTP client.
        """
        try:
            await self._client.aclose()
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("Error closing BinanceRestClient: %s", exc)

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> List[List[Any]]:
        """
        Fetch klines (candlestick data) from Binance.

        Wrapper for GET /api/v3/klines:
          - https://api.binance.com/api/v3/klines

        :param symbol: Trading pair, e.g. 'ETHUSDT' (case-insensitive).
        :param interval: Interval string, e.g. '1m'.
        :param start_time: Start time in ms (optional).
        :param end_time: End time in ms (optional).
        :param limit: Max number of klines [1, 1000].
        :return: List of klines in Binance raw format.
        """
        symbol = symbol.upper()
        limit = max(1, min(limit, 1000))

        params: Dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = int(start_time)
        if end_time is not None:
            params["endTime"] = int(end_time)

        url = "/api/v3/klines"

        backoff = 1.0
        backoff_max = 10.0

        for attempt in range(1, self._max_retries + 1):
            try:
                resp = await self._client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

                # Binance returns a list of lists; we just pass it through.
                if not isinstance(data, list):
                    self._logger.warning(
                        "Unexpected klines response format for %s: %s",
                        symbol,
                        type(data),
                    )
                    return []

                return data

            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                self._logger.warning(
                    "Binance klines timeout/connect error (attempt %s/%s) for %s: %s",
                    attempt,
                    self._max_retries,
                    symbol,
                    exc,
                )
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                # 4xx -> não adianta retry (exceto 429/418 rate limit)
                if status not in (418, 429) and 400 <= status < 500:
                    self._logger.error(
                        "Binance klines HTTP %s for %s (no retry): %s",
                        status,
                        symbol,
                        exc,
                    )
                    return []
                self._logger.warning(
                    "Binance klines HTTP %s (attempt %s/%s) for %s: %s",
                    status,
                    attempt,
                    self._max_retries,
                    symbol,
                    exc,
                )
            except Exception as exc:  # noqa: BLE001
                self._logger.warning(
                    "Binance klines unexpected error (attempt %s/%s) for %s: %s",
                    attempt,
                    self._max_retries,
                    symbol,
                    exc,
                )

            if attempt == self._max_retries:
                break

            # Exponential backoff + jitter
            jitter = random.uniform(0, 0.5)
            sleep_for = min(backoff, backoff_max) + jitter
            await asyncio.sleep(sleep_for)
            backoff = min(backoff * 2, backoff_max)

        self._logger.error(
            "Failed to fetch klines for %s after %s attempts.",
            symbol,
            self._max_retries,
        )
        return []
