import logging
from typing import Any, Dict, Optional

import httpx


class MarketDataHttpClient:
    """
    Thin async HTTP client for api-market-data.

    This client exists so api-signals can optionally fetch the required data
    when api-market-data triggers a candle close event without embedding the
    full payload.

    Design goals:
      - Small surface area (only what signals needs).
      - Safe defaults and best-effort error handling (returns None on failure).
      - Paths are configurable because api-market-data may evolve with more resources.

    Expected usage patterns:
      1) Recommended: api-market-data POSTs the full indicator_snapshot (and optionally indicator_set).
      2) Fallback: api-market-data POSTs only identifiers (indicator_set_id + ts) and api-signals fetches details here.
    """

    def __init__(
        self,
        base_url: str,
        timeout_sec: float = 30.0,
        indicator_set_path_tpl: str = "/api/indicator-sets/{indicator_set_id}",
        indicator_snapshot_path_tpl: str = "/api/indicator-snapshots/{indicator_set_id}/{ts}",
    ):
        self._base_url = (base_url or "").rstrip("/")
        self._timeout = float(timeout_sec)
        self._logger = logging.getLogger(self.__class__.__name__)

        # Templates allow api-market-data to change routes without refactoring api-signals.
        self._indicator_set_path_tpl = indicator_set_path_tpl
        self._indicator_snapshot_path_tpl = indicator_snapshot_path_tpl

    def _url(self, path: str) -> str:
        return f"{self._base_url}{path}"

    async def get_indicator_set(self, indicator_set_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch an indicator set document from api-market-data.

        Args:
            indicator_set_id: Logical identifier (cfg_hash) of the indicator set.

        Returns:
            A dict payload (projection without _id is fine), or None if unavailable.
        """
        if not self._base_url:
            self._logger.warning("MarketDataHttpClient base_url is empty; cannot fetch indicator set.")
            return None

        path = self._indicator_set_path_tpl.format(indicator_set_id=indicator_set_id)
        url = self._url(path)

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                r = await client.get(url)
                if r.status_code == 200:
                    return r.json()
                self._logger.warning("get_indicator_set non-200 %s: %s %s", url, r.status_code, r.text)
        except Exception as exc:
            self._logger.exception("get_indicator_set error for %s: %s", url, exc)

        return None

    async def get_indicator_snapshot(self, indicator_set_id: str, ts: int) -> Optional[Dict[str, Any]]:
        """
        Fetch a single indicator snapshot (for a closed candle) from api-market-data.

        Args:
            indicator_set_id: Logical identifier (cfg_hash) of the indicator set.
            ts: Candle close timestamp (ms).

        Returns:
            A dict with the snapshot fields required by EvaluateActiveStrategiesUseCase,
            or None if unavailable.
        """
        if not self._base_url:
            self._logger.warning("MarketDataHttpClient base_url is empty; cannot fetch indicator snapshot.")
            return None

        path = self._indicator_snapshot_path_tpl.format(indicator_set_id=indicator_set_id, ts=int(ts))
        url = self._url(path)

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                r = await client.get(url)
                if r.status_code == 200:
                    return r.json()
                self._logger.warning("get_indicator_snapshot non-200 %s: %s %s", url, r.status_code, r.text)
        except Exception as exc:
            self._logger.exception("get_indicator_snapshot error for %s: %s", url, exc)

        return None
