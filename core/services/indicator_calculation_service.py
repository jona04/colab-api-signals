from typing import Dict, List, Tuple, Optional

import pandas as pd


class IndicatorCalculationService:
    """
    Stateless helper for computing EMA (fast/slow) and ATR% over a window of candles.

    This implementation expects a list of candle documents with keys:
    ['open','high','low','close','close_time'].
    """

    @staticmethod
    def compute_ema(series: pd.Series, span: int) -> pd.Series:
        """
        Compute EMA for a given pandas Series with a minimum effective warm-up.
        """
        min_periods = max(2, span // 2)
        return series.ewm(span=span, adjust=False, min_periods=min_periods).mean()

    @staticmethod
    def compute_atr_pct(df: pd.DataFrame, window: int) -> pd.Series:
        """
        Compute ATR% = ATR / Close using True Range and EMA-like smoothing.
        """
        h = pd.to_numeric(df["high"], errors="coerce")
        l = pd.to_numeric(df["low"], errors="coerce")
        c = pd.to_numeric(df["close"], errors="coerce").ffill()
        prev_c = c.shift(1)
        tr = pd.concat([(h - l).abs(), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
        min_periods = max(2, window // 2)
        atr = tr.ewm(span=window, adjust=False, min_periods=min_periods).mean()
        atr_pct = (atr / c).ffill().fillna(0.0)
        return atr_pct

    def compute_snapshot_for_last(
        self,
        candles: List[Dict],
        ema_fast: int,
        ema_slow: int,
        atr_window: int,
    ) -> Optional[Dict]:
        """
        Given the last N candles (ascending by close_time), compute EMA fast/slow and ATR%
        and return a snapshot for the last candle including OHLC and indicators.

        :param candles: List of candle dicts (ascending by close_time).
        :param ema_fast: EMA fast period.
        :param ema_slow: EMA slow period.
        :param atr_window: ATR window (bars).
        :return: Snapshot dict or None if not enough data.
        """
        required = max(ema_slow, atr_window)
        if len(candles) < required:
            return None

        df = pd.DataFrame(candles)
        # ensure numeric
        df["close"] = pd.to_numeric(df["close"], errors="coerce").ffill()
        df["high"] = pd.to_numeric(df["high"], errors="coerce")
        df["low"] = pd.to_numeric(df["low"], errors="coerce")
        df["open"] = pd.to_numeric(df["open"], errors="coerce")

        ema_fast_s = self.compute_ema(df["close"], ema_fast)
        ema_slow_s = self.compute_ema(df["close"], ema_slow)
        atr_pct_s = self.compute_atr_pct(df, atr_window)

        last = df.iloc[-1]
        snapshot = {
            "symbol": candles[-1]["symbol"],
            "ts": int(last["close_time"]),
            # include OHLC for convenience / denormalized read
            "open": float(last["open"]),
            "high": float(last["high"]),
            "low": float(last["low"]),
            "close": float(last["close"]),
            # indicators
            "ema_fast": float(ema_fast_s.iloc[-1]),
            "ema_slow": float(ema_slow_s.iloc[-1]),
            "atr_pct": float(atr_pct_s.iloc[-1]),
        }
        return snapshot
