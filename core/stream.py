"""
core/stream.py — Real-time price stream via Alpaca WebSocket

Connects to Alpaca's free IEX WebSocket feed and maintains a live
price cache that the strategy reads from instead of polling bars.

Architecture:
  - Startup: fetches 5-min bars to seed initial ATR, EMA, VWAP, RSI
  - Live: WebSocket ticks update EMA, VWAP every tick
  - Candle builder: accumulates ticks into 1-minute candles in real time.
    When each 1-minute candle closes, ATR and RSI are recalculated from
    real high/low/close data — giving accurate indicators that update
    every minute instead of being corrupted by tick noise.

Why candles for RSI (and ATR):
  RSI requires consistent close-to-close deltas over N periods.
  Individual ticks are not closes — they're random prints that can
  swing RSI to 0 or 100 on consecutive ticks moving the same direction.
  Calculating RSI on 1-min candle closes gives the real 14-period RSI
  that matches what TradingView and every other platform shows.

v2 additions:
  - get_candle_volumes(symbol): returns completed candle volumes
  - get_elapsed_seconds(symbol): seconds elapsed since current candle opened
  - get_candle_minute(symbol): current candle's minute boundary

v2.1 additions:
  - get_current_candle_volume(symbol): accumulated volume for in-progress candle

v3 fix (RSI):
  - RSI now calculated on 1-minute candle closes only, not on every tick
  - Eliminates RSI swinging 5-100 within seconds due to tick noise
  - on_tick() no longer updates RSI; candle close event updates it instead
  - _closes deque still maintained for EMA (tick-level EMA is acceptable
    since EMA smooths naturally; RSI does not)
"""

import json
import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta

import requests
import websocket

import config

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

WS_URL          = "wss://stream.data.alpaca.markets/v2/iex"
BARS_URL        = "https://data.alpaca.markets/v2/stocks/{symbol}/bars"
STALE_SECONDS   = 120
RECONNECT_DELAY = 10
BAR_LIMIT       = 100
BAR_TIMEFRAME   = "5Min"
CANDLE_MINUTES  = 1
ATR_PERIOD      = 14
RSI_PERIOD      = 14   # number of 1-min candle closes needed for RSI


# ── Indicator math ────────────────────────────────────────────────────────────

def _ema(prices: list, period: int) -> float:
    if not prices:
        return 0.0
    if len(prices) < period:
        return sum(prices) / len(prices)
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return ema


def _atr(highs: list, lows: list, closes: list, period: int = 14) -> float:
    if not highs or not lows or not closes:
        return 1.0
    if len(closes) < 2:
        return abs(highs[-1] - lows[-1]) or 1.0
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1])
        )
        trs.append(tr)
    period = min(period, len(trs))
    result = sum(trs[-period:]) / period
    return result if result > 0 else 1.0


def _rsi_from_closes(closes: list, period: int = 14) -> float:
    """
    Calculate RSI from a list of candle close prices.

    Requires at least period+1 values to compute one delta.
    Returns 50.0 (neutral) if not enough data.

    This should ONLY be called with candle closes (1-min or 5-min),
    never with individual tick prices — tick-level RSI is meaningless.
    """
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _vwap(prices: list, volumes: list) -> float:
    if not prices or not volumes:
        return prices[-1] if prices else 0.0
    total_vol = sum(volumes)
    if total_vol == 0:
        return prices[-1]
    return sum(p * v for p, v in zip(prices, volumes)) / total_vol


# ── 1-minute candle builder ───────────────────────────────────────────────────

class CandleBuilder:
    """
    Accumulates tick data into 1-minute OHLC candles.

    When a candle closes (minute boundary crossed), it's added to the
    candle history. ATR and RSI are recalculated from real OHLC data
    on each candle close — not on every tick.

    v3: exposes get_rsi() so SymbolCache can read candle-based RSI
    instead of computing it from noisy tick prices.
    """

    def __init__(self, atr_period: int = ATR_PERIOD, rsi_period: int = RSI_PERIOD):
        self.atr_period = atr_period
        self.rsi_period = rsi_period

        _candle_maxlen = max(atr_period + rsi_period + 10, 80)
        self._candle_highs   = deque(maxlen=_candle_maxlen)
        self._candle_lows    = deque(maxlen=_candle_maxlen)
        self._candle_closes  = deque(maxlen=_candle_maxlen)
        self._candle_volumes = deque(maxlen=_candle_maxlen)

        self._candle_open    = None
        self._candle_high    = None
        self._candle_low     = None
        self._candle_close   = None
        self._candle_volume  = 0
        self._candle_minute  = None
        self._candle_start   = None

        # Cached indicator values — updated on candle close only
        self._atr = 1.0
        self._rsi = 50.0

        self._lock = threading.Lock()

    def seed_from_bars(self, highs: list, lows: list, closes: list,
                       volumes: list = None):
        with self._lock:
            self._candle_highs.extend(highs)
            self._candle_lows.extend(lows)
            self._candle_closes.extend(closes)
            if volumes:
                self._candle_volumes.extend(volumes)
            # Compute initial RSI and ATR from seeded bar data
            self._atr = _atr(
                list(self._candle_highs),
                list(self._candle_lows),
                list(self._candle_closes),
                self.atr_period
            )
            self._rsi = _rsi_from_closes(list(self._candle_closes), self.rsi_period)

    def on_tick(self, price: float, volume: int, ts: datetime) -> bool:
        minute = ts.replace(second=0, microsecond=0)
        candle_closed = False

        with self._lock:
            if self._candle_minute is None:
                self._candle_minute = minute
                self._candle_start  = datetime.utcnow()
                self._candle_open   = price
                self._candle_high   = price
                self._candle_low    = price
                self._candle_close  = price
                self._candle_volume = volume

            elif minute > self._candle_minute:
                # Candle closed — save it and update indicators
                self._candle_highs.append(self._candle_high)
                self._candle_lows.append(self._candle_low)
                self._candle_closes.append(self._candle_close)
                self._candle_volumes.append(self._candle_volume)

                # Update ATR and RSI from candle closes — NOT tick prices
                h = list(self._candle_highs)
                l = list(self._candle_lows)
                c = list(self._candle_closes)
                self._atr = _atr(h, l, c, self.atr_period)
                self._rsi = _rsi_from_closes(c, self.rsi_period)

                # Start new candle
                self._candle_minute = minute
                self._candle_start  = datetime.utcnow()
                self._candle_open   = price
                self._candle_high   = price
                self._candle_low    = price
                self._candle_close  = price
                self._candle_volume = volume

                candle_closed = True

            else:
                if price > self._candle_high:
                    self._candle_high = price
                if price < self._candle_low:
                    self._candle_low = price
                self._candle_close  = price
                self._candle_volume += volume

        return candle_closed

    def get_atr(self) -> float:
        with self._lock:
            return self._atr

    def get_rsi(self) -> float:
        """
        Returns RSI calculated from 1-minute candle closes.
        This is the real RSI — stable, meaningful, matches charting platforms.
        Never call _rsi_from_closes() on tick prices.
        """
        with self._lock:
            return self._rsi

    def get_candles(self, n: int = 10) -> list:
        with self._lock:
            highs   = list(self._candle_highs)
            lows    = list(self._candle_lows)
            closes  = list(self._candle_closes)
            volumes = list(self._candle_volumes)

        count = min(len(highs), len(lows), len(closes))
        if count == 0:
            return []

        candles = [
            {
                "high":   highs[i],
                "low":    lows[i],
                "close":  closes[i],
                "volume": volumes[i] if i < len(volumes) else 0,
            }
            for i in range(count)
        ]
        return candles[-n:] if len(candles) >= n else candles

    def get_candle_volumes(self, n: int = 20) -> list:
        with self._lock:
            vols = list(self._candle_volumes)
        return vols[-n:] if len(vols) >= n else vols

    def get_elapsed_seconds(self) -> float:
        with self._lock:
            if self._candle_start is None:
                return 30.0
            elapsed = (datetime.utcnow() - self._candle_start).total_seconds()
            return max(elapsed, 1.0)

    def get_candle_minute(self):
        with self._lock:
            return self._candle_minute

    def get_current_candle_volume(self) -> int:
        with self._lock:
            return self._candle_volume

    def candle_count(self) -> int:
        with self._lock:
            return len(self._candle_closes)


# ── Price cache entry ─────────────────────────────────────────────────────────

class SymbolCache:
    """
    Holds all indicator state for one symbol.

    EMA, VWAP: updated every tick (naturally smoothed, tick noise is fine)
    ATR, RSI:  updated every minute on candle close (require real OHLC)
    High/Low:  tracked from ticks for price action signal

    v3: RSI is now read from CandleBuilder.get_rsi() instead of being
    calculated from tick prices. This fixes RSI swinging 5-100 in seconds.
    """

    def __init__(self, symbol: str):
        self.symbol     = symbol
        self.price      = None
        self.volume     = 0
        self.updated_at = None

        self.ema9       = None
        self.ema21      = None
        self.atr        = None
        self.rsi        = 50.0   # default neutral until first candle closes
        self.vwap       = None
        self.high       = None
        self.low        = None
        self.prev_high  = None
        self.prev_low   = None

        self._closes        = deque(maxlen=200)   # tick closes for EMA only
        self._intra_prices  = deque(maxlen=5000)
        self._intra_volumes = deque(maxlen=5000)

        self._candles = CandleBuilder(atr_period=ATR_PERIOD, rsi_period=RSI_PERIOD)

        self._lock = threading.Lock()

    def seed_from_bars(self, bars: list):
        if not bars:
            log.warning(f"[STREAM] No bars to seed {self.symbol}")
            return

        closes  = [b["c"] for b in bars]
        highs   = [b["h"] for b in bars]
        lows    = [b["l"] for b in bars]
        vols    = [b["v"] for b in bars]

        with self._lock:
            self._closes.extend(closes)
            self.ema9  = _ema(closes, 9)
            self.ema21 = _ema(closes, 21)
            self.vwap  = _vwap(closes, vols)
            self.price = closes[-1]

            self.high      = max(highs[-20:])
            self.low       = min(lows[-20:])
            self.prev_high = max(highs[-21:-1]) if len(highs) > 20 else self.high
            self.prev_low  = min(lows[-21:-1])  if len(lows) > 20  else self.low

            today = datetime.utcnow().date()
            for b in bars:
                bar_time = b.get("t", "")
                if str(today) in str(bar_time):
                    self._intra_prices.append(b["c"])
                    self._intra_volumes.append(b["v"])

            self.updated_at = datetime.utcnow()

        # Seed candle builder — this also computes initial ATR and RSI
        self._candles.seed_from_bars(highs, lows, closes, vols)
        self.atr = self._candles.get_atr()
        self.rsi = self._candles.get_rsi()

        log.info(
            f"[STREAM] {self.symbol} seeded | "
            f"price={self.price:.2f} EMA9={self.ema9:.2f} "
            f"EMA21={self.ema21:.2f} ATR={self.atr:.4f} RSI={self.rsi:.1f} "
            f"candles={self._candles.candle_count()}"
        )

    def on_tick(self, price: float, volume: int, ts: datetime):
        with self._lock:
            self.price      = price
            self.volume     = volume
            self.updated_at = datetime.utcnow()

            self._closes.append(price)
            self._intra_prices.append(price)
            self._intra_volumes.append(volume if volume > 0 else 1)

            closes = list(self._closes)

            # EMA updates on every tick — smoothing makes this acceptable
            if len(closes) >= 9:
                self.ema9 = _ema(closes, 9)
            if len(closes) >= 21:
                self.ema21 = _ema(closes, 21)

            # NOTE: RSI is NOT updated here anymore (v3 fix)
            # RSI is updated only when a candle closes in the block below

            ip = list(self._intra_prices)
            iv = list(self._intra_volumes)
            if ip:
                self.vwap = _vwap(ip, iv)

            if self.high is None or price > self.high:
                self.high = price
            if self.low is None or price < self.low:
                self.low = price

        # Feed candle builder — returns True when a new candle closes
        candle_closed = self._candles.on_tick(price, volume, ts)

        if candle_closed:
            # Update ATR and RSI from candle closes — real values
            new_atr = self._candles.get_atr()
            new_rsi = self._candles.get_rsi()
            with self._lock:
                self.atr = new_atr
                self.rsi = new_rsi
            log.debug(
                f"[STREAM] {self.symbol} candle closed — "
                f"ATR={new_atr:.4f} RSI={new_rsi:.1f} "
                f"({self._candles.candle_count()} candles)"
            )

    def get_candles(self, n: int = 10) -> list:
        return self._candles.get_candles(n)

    def get_candle_volumes(self, n: int = 20) -> list:
        return self._candles.get_candle_volumes(n)

    def get_elapsed_seconds(self) -> float:
        return self._candles.get_elapsed_seconds()

    def get_candle_minute(self):
        return self._candles.get_candle_minute()

    def get_current_candle_volume(self) -> int:
        return self._candles.get_current_candle_volume()

    def to_dict(self) -> dict:
        with self._lock:
            stale = (
                self.updated_at is None or
                (datetime.utcnow() - self.updated_at).total_seconds() > STALE_SECONDS
            )
            return {
                "price":      self.price,
                "volume":     self.volume,
                "ema9":       self.ema9,
                "ema21":      self.ema21,
                "atr":        self.atr,
                "rsi":        self.rsi,
                "vwap":       self.vwap,
                "high":       self.high,
                "low":        self.low,
                "prev_high":  self.prev_high,
                "prev_low":   self.prev_low,
                "updated_at": self.updated_at,
                "stale":      stale,
            }


# ── Main stream class ─────────────────────────────────────────────────────────

class PriceStream:
    """
    Manages the Alpaca WebSocket connection and symbol price caches.
    Runs entirely in background threads — non-blocking for the main bot loop.
    """

    def __init__(self, symbols: list):
        self.symbols     = [s.upper() for s in symbols]
        self._cache      = {s: SymbolCache(s) for s in self.symbols}
        self._ws         = None
        self._running    = False
        self._thread     = None
        self._ready      = threading.Event()
        self._subscribed = False

    # ── Public API ────────────────────────────────────────────

    def start(self):
        log.info(f"[STREAM] Starting for {self.symbols}")
        self._seed_all()
        self._running = True
        self._thread  = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        if self._ready.wait(timeout=20):
            log.info("[STREAM] ✓ WebSocket live and subscribed")
        else:
            log.warning("[STREAM] WebSocket not ready after 20s — using seeded data")

    def stop(self):
        self._running = False
        if self._ws:
            self._ws.close()

    def get_price(self, symbol: str) -> dict:
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        if cache is None:
            return None
        return cache.to_dict()

    def is_ready(self) -> bool:
        return self._ready.is_set()

    def is_stale(self, symbol: str) -> bool:
        data = self.get_price(symbol)
        return data is None or data["stale"]

    def get_candles(self, symbol: str, n: int = 10) -> list:
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        if cache is None:
            return []
        return cache.get_candles(n)

    def get_candle_volumes(self, symbol: str, n: int = 20) -> list:
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        if cache is None:
            return []
        return cache.get_candle_volumes(n)

    def get_elapsed_seconds(self, symbol: str) -> float:
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        if cache is None:
            return 30.0
        return cache.get_elapsed_seconds()

    def get_candle_minute(self, symbol: str):
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        if cache is None:
            return None
        return cache.get_candle_minute()

    def get_current_candle_volume(self, symbol: str) -> int:
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        if cache is None:
            return 0
        return cache.get_current_candle_volume()

    # ── Bar seeding ───────────────────────────────────────────

    def _seed_all(self):
        for symbol in self.symbols:
            try:
                bars = self._fetch_bars(symbol)
                self._cache[symbol].seed_from_bars(bars)
            except Exception as e:
                log.error(f"[STREAM] Failed to seed {symbol}: {e}")

    def _fetch_bars(self, symbol: str) -> list:
        end   = datetime.utcnow()
        start = end - timedelta(hours=10)

        resp = requests.get(
            BARS_URL.format(symbol=symbol),
            headers={
                "APCA-API-KEY-ID":     config.ALPACA_API_KEY,
                "APCA-API-SECRET-KEY": config.ALPACA_SECRET_KEY,
            },
            params={
                "timeframe": BAR_TIMEFRAME,
                "start":     start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end":       end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "limit":     BAR_LIMIT,
                "feed":      "iex",
            },
            timeout=15,
        )
        resp.raise_for_status()
        bars = resp.json().get("bars") or []
        log.info(f"[STREAM] Fetched {len(bars)} {BAR_TIMEFRAME} bars for {symbol}")
        return bars

    # ── WebSocket loop ────────────────────────────────────────

    def _run_loop(self):
        while self._running:
            try:
                self._subscribed = False
                self._connect()
            except Exception as e:
                log.error(f"[STREAM] WebSocket error: {e}")
            if self._running:
                log.info(f"[STREAM] Reconnecting in {RECONNECT_DELAY}s...")
                self._ready.clear()
                time.sleep(RECONNECT_DELAY)

    def _connect(self):
        log.info(f"[STREAM] Connecting to {WS_URL}")
        self._ws = websocket.WebSocketApp(
            WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws.run_forever(ping_interval=30, ping_timeout=10)

    # ── WebSocket event handlers ──────────────────────────────

    def _on_open(self, ws):
        log.info("[STREAM] WebSocket connected — authenticating")
        ws.send(json.dumps({
            "action": "auth",
            "key":    config.ALPACA_API_KEY,
            "secret": config.ALPACA_SECRET_KEY,
        }))

    def _on_message(self, ws, raw):
        try:
            messages = json.loads(raw)
            for msg in messages:
                self._handle_message(ws, msg)
        except Exception as e:
            log.error(f"[STREAM] Message parse error: {e}")

    def _handle_message(self, ws, msg: dict):
        t = msg.get("T")

        if t == "success" and msg.get("msg") == "connected":
            log.info("[STREAM] Connected to Alpaca stream")

        elif t == "success" and msg.get("msg") == "authenticated":
            log.info("[STREAM] Authenticated ✓ — subscribing")
            ws.send(json.dumps({
                "action": "subscribe",
                "trades": self.symbols,
                "quotes": [],
                "bars":   [],
            }))

        elif t == "subscription":
            trades = msg.get("trades", [])
            log.info(f"[STREAM] Subscribed ✓ — receiving trades for {trades}")
            self._subscribed = True
            self._ready.set()

        elif t == "error":
            code = msg.get("code")
            err  = msg.get("msg", "unknown")
            log.error(f"[STREAM] Error {code}: {err}")
            if code == 406:
                log.warning("[STREAM] Connection limit (406) — closing and retrying")
                ws.close()

        elif t == "t":
            symbol = msg.get("S", "").upper()
            price  = msg.get("p")
            size   = msg.get("s", 0)

            if symbol not in self._cache or not price:
                return

            raw_ts = msg.get("t", "")
            try:
                ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                ts = ts.replace(tzinfo=None)
            except Exception:
                ts = datetime.utcnow()

            self._cache[symbol].on_tick(float(price), int(size), ts)

        else:
            if t is not None:
                log.debug(f"[STREAM] Unhandled message type '{t}': {msg}")

    def _on_error(self, ws, error):
        log.error(f"[STREAM] WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        log.warning(f"[STREAM] WebSocket closed — code={code} msg={msg}")
        self._ready.clear()
        self._subscribed = False
