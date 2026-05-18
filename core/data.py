"""
core/data.py — Market Data via Alpaca API

Fetches OHLCV bars for QQQ (simulating MNQ micro futures behavior
with leverage multiplier applied at the strategy/risk layer).

Key feature: STALENESS CHECK
IEX feed has a 15-minute delay and can serve the same stale bar
repeatedly during low-volume periods. Before returning data we check
the timestamp of the most recent bar. If it is older than
MAX_BAR_AGE_MINUTES we return None so the bot skips that cycle
rather than trading on frozen data.

Calculates all indicators:
  EMA fast/slow, VWAP (resets daily), RSI, ATR, Volume spike
"""

import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
import requests
import config

log = logging.getLogger(__name__)

ALPACA_DATA_URL = "https://data.alpaca.markets/v2"


class DataFetcher:
    def __init__(self):
        self._price_cache  = {}
        self._stale_warned = {}   # symbol -> last time we logged a stale warning
        self._session = requests.Session()
        self._session.headers.update({
            "APCA-API-KEY-ID":     config.ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": config.ALPACA_SECRET_KEY,
            "Content-Type":        "application/json"
        })
        log.info("[DATA] Alpaca data fetcher initialized ✓")

    def get_bars(self, symbol: str, limit=200):
        """
        Fetch 1-minute OHLCV bars from Alpaca IEX feed.

        Returns DataFrame or None.
        Returns None (skips) if the most recent bar is older than
        MAX_BAR_AGE_MINUTES — protects against trading on stale data.
        """
        try:
            end   = datetime.utcnow() - timedelta(minutes=1)
            start = end - timedelta(hours=6)

            r = self._session.get(
                f"{ALPACA_DATA_URL}/stocks/{symbol}/bars",
                params={
                    "timeframe": "1Min",
                    "start":     start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "end":       end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "limit":     limit,
                    "feed":      "iex"
                },
                timeout=10
            )
            r.raise_for_status()
            bars = r.json().get("bars", [])

            if not bars:
                log.warning(f"[DATA] No bars returned for {symbol}")
                return None

            df = pd.DataFrame(bars)
            df["timestamp"] = pd.to_datetime(df["t"], utc=True)
            df = df.set_index("timestamp").sort_index()
            df = df.rename(columns={
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
                "v": "volume"
            })
            df = df[["open", "high", "low", "close", "volume"]].astype(float)

            # ── Staleness check ───────────────────────────────────
            # IEX can freeze and return the same bar repeatedly.
            # Check how old the most recent bar is.
            last_bar_time = df.index[-1]   # timezone-aware UTC timestamp
            now_utc       = datetime.now(timezone.utc)
            age_minutes   = (now_utc - last_bar_time).total_seconds() / 60

            max_age = getattr(config, "MAX_BAR_AGE_MINUTES", 20)

            if age_minutes > max_age:
                # only log once per 5 minutes to avoid spam
                last_warn = self._stale_warned.get(symbol, 0)
                import time
                if time.time() - last_warn > 300:
                    log.warning(
                        f"[DATA] {symbol} data is stale — "
                        f"last bar was {age_minutes:.1f} min ago "
                        f"(max allowed: {max_age} min). Skipping."
                    )
                    self._stale_warned[symbol] = time.time()
                return None

            return df

        except Exception as e:
            log.error(f"[DATA] Bar fetch failed {symbol}: {e}")
            return None

    def get_latest_price(self, symbol: str):
        """
        Get the latest trade price for a symbol.
        Falls back to cached price if the request fails.
        """
        try:
            r = self._session.get(
                f"{ALPACA_DATA_URL}/stocks/{symbol}/trades/latest",
                params={"feed": "iex"},
                timeout=5
            )
            r.raise_for_status()
            price = float(r.json()["trade"]["p"])
            if price > 0:
                self._price_cache[symbol] = price
            return price
        except Exception as e:
            log.debug(f"[DATA] Price fetch failed {symbol}: {e}")
            return self._price_cache.get(symbol)

    def calculate_indicators(self, df):
        """
        Adds all indicators to the dataframe.
        Single source of truth for all signal calculations.
        """
        if df is None or len(df) < 50:
            return None

        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        # EMAs
        df["ema_fast"]  = close.ewm(span=config.EMA_FAST, adjust=False).mean()
        df["ema_slow"]  = close.ewm(span=config.EMA_SLOW, adjust=False).mean()
        ema_above       = (df["ema_fast"] > df["ema_slow"]).astype(int)
        df["ema_cross"] = ema_above.diff()

        # VWAP — resets daily
        tp               = (high + low + close) / 3
        df["tp_vol"]     = tp * volume
        df["date"]       = df.index.date
        df["cum_tp_vol"] = df.groupby("date")["tp_vol"].cumsum()
        df["cum_vol"]    = df.groupby("date")["volume"].cumsum()
        df["vwap"]       = df["cum_tp_vol"] / df["cum_vol"].replace(0, np.nan)

        # RSI
        delta    = close.diff()
        gain     = delta.clip(lower=0)
        loss     = (-delta).clip(lower=0)
        avg_gain = gain.ewm(span=config.RSI_PERIOD, adjust=False).mean()
        avg_loss = loss.ewm(span=config.RSI_PERIOD, adjust=False).mean()
        rs       = avg_gain / avg_loss.replace(0, np.nan)
        df["rsi"] = 100 - (100 / (1 + rs))

        # ATR
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs()
        ], axis=1).max(axis=1)
        df["atr"] = tr.ewm(span=config.ATR_PERIOD, adjust=False).mean()

        # Volume
        df["vol_avg"]   = volume.rolling(20).mean()
        df["vol_spike"] = volume / df["vol_avg"].replace(0, np.nan)

        # Price action
        df["hh"] = high > high.shift(1)
        df["hl"] = low  > low.shift(1)
        df["lh"] = high < high.shift(1)
        df["ll"] = low  < low.shift(1)

        return df

    def get_full_snapshot(self, symbol: str):
        """Fetch bars + calculate all indicators. Returns None if stale."""
        df = self.get_bars(symbol)
        if df is None:
            return None
        return self.calculate_indicators(df)
