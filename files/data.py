"""
core/data.py — Market Data via Tradovate API

Fetches OHLCV bars for MNQ and MES micro futures.
Handles session token auth automatically — tokens expire every 60-80 min
so we refresh proactively every 45 minutes.

Calculates all indicators in one place:
  EMA fast/slow, VWAP (resets daily), RSI, ATR, Volume spike
"""

import time
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import config

log = logging.getLogger(__name__)

# Tradovate endpoints
DEMO_AUTH_URL = "https://demo.tradovateapi.com/v1/auth/accesstokenrequest"
DEMO_BASE_URL = "https://demo.tradovateapi.com/v1"
MD_BASE_URL   = "https://md.tradovateapi.com/v1"


class DataFetcher:
    def __init__(self):
        self._token        = None
        self._token_expiry = 0
        self._price_cache  = {}
        self._session      = requests.Session()
        self._authenticate()
        log.info("[DATA] Tradovate data fetcher initialized")

    # Auth
    def _authenticate(self):
        payload = {
            "name":        config.TRADOVATE_USERNAME,
            "password":    config.TRADOVATE_PASSWORD,
            "appId":       config.TRADOVATE_APP_ID,
            "appVersion":  "1.0",
            "cid":         config.TRADOVATE_CID,
            "sec":         config.TRADOVATE_SECRET,
            "deviceId":    "alphabot-001"
        }
        r = self._session.post(DEMO_AUTH_URL, json=payload, timeout=10)
        r.raise_for_status()
        data = r.json()
        if "accessToken" not in data:
            raise ValueError(f"No token in response: {data}")
        self._token        = data["accessToken"]
        self._token_expiry = time.time() + (45 * 60)
        self._session.headers.update({
            "Authorization": f"Bearer {self._token}",
            "Content-Type":  "application/json"
        })
        log.info("[DATA] Tradovate authenticated")

    def _ensure_token(self):
        if time.time() >= self._token_expiry:
            log.info("[DATA] Token expiring, refreshing...")
            self._authenticate()

    def get_bars(self, symbol: str, limit=200):
        self._ensure_token()
        try:
            r = self._session.post(
                f"{MD_BASE_URL}/chart/subscribe",
                json={
                    "symbol": symbol,
                    "chartDescription": {
                        "underlyingType": "MinuteBar",
                        "elementSize": 1,
                        "elementSizeUnit": "UnderlyingUnits",
                        "withHistogram": False
                    },
                    "timeRange": {
                        "asMuchAsElements": limit
                    }
                },
                timeout=15
            )
            r.raise_for_status()
            data = r.json()
            bars = data.get("bars", [])
            if not bars:
                log.warning(f"[DATA] No bars for {symbol}")
                return None

            df = pd.DataFrame(bars)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df = df.set_index("timestamp").sort_index()

            # combine up/down volume
            up   = df.get("upVolume",   pd.Series(0, index=df.index))
            down = df.get("downVolume", pd.Series(0, index=df.index))
            df["volume"] = up.fillna(0) + down.fillna(0)

            return df[["open", "high", "low", "close", "volume"]].astype(float)

        except Exception as e:
            log.error(f"[DATA] Bar fetch failed {symbol}: {e}")
            return None

    def get_latest_price(self, symbol: str):
        self._ensure_token()
        try:
            r = self._session.get(
                f"{MD_BASE_URL}/quote/find",
                params={"name": symbol},
                timeout=5
            )
            r.raise_for_status()
            data  = r.json()
            price = float(data.get("last", data.get("bid", 0)))
            if price > 0:
                self._price_cache[symbol] = price
            return price
        except Exception as e:
            log.debug(f"[DATA] Price fetch failed {symbol}: {e}")
            return self._price_cache.get(symbol)

    def calculate_indicators(self, df):
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

        # VWAP - resets daily
        tp             = (high + low + close) / 3
        df["tp_vol"]   = tp * volume
        df["date"]     = df.index.date
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
        df = self.get_bars(symbol)
        if df is None:
            return None
        return self.calculate_indicators(df)
