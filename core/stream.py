"""
core/stream.py — Real-time price stream via Alpaca WebSocket

Connects to Alpaca's free IEX WebSocket feed and maintains a live
price cache that the strategy reads from instead of polling bars.

Architecture:
  - Startup: fetches 5-min bars to seed initial ATR, EMA, VWAP, RSI
  - Live: WebSocket ticks update EMA, RSI, VWAP every tick
  - Candle builder: accumulates ticks into 1-minute candles in real time.
    When each 1-minute candle closes, ATR is recalculated from real
    high/low/close data — giving live ATR that updates every minute
    instead of being frozen at startup.

Why candles for ATR:
  ATR requires a bar's high/low range — a single tick has no range.
  We build our own 1-min candles from the tick stream so ATR updates
  throughout the day using real intraday volatility, not stale morning data.

The strategy reads from stream.get_price(symbol).
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
BAR_TIMEFRAME   = "5Min"   # seed ATR from 5-min bars for realistic volatility
CANDLE_MINUTES  = 1        # build candles every N minutes from ticks
ATR_PERIOD      = 14       # number of candles to use for ATR

# ── Indicator math ────────────────────────────────────────────────────────────

def _ema(prices: list, period: int) -> float:
    """Exponential moving average."""
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
    """
    True Average True Range from OHLC candle data.
    Requires at least 2 candles. Falls back to HL range if fewer.
    """
    if not highs or not lows or not closes:
        return 1.0   # safe fallback — never return 0
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


def _rsi(closes: list, period: int = 14) -> float:
    """RSI from close prices."""
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
    """Volume weighted average price."""
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
    candle history and ATR is recalculated from real high/low data.

    This gives us live, accurate ATR that updates every minute using
    actual intraday volatility — not stale morning bar data.
    """

    def __init__(self, atr_period: int = ATR_PERIOD):
        self.atr_period = atr_period

        # completed candle history for ATR calculation
        self._candle_highs   = deque(maxlen=atr_period + 5)
        self._candle_lows    = deque(maxlen=atr_period + 5)
        self._candle_closes  = deque(maxlen=atr_period + 5)

        # current open candle
        self._candle_open    = None
        self._candle_high    = None
        self._candle_low     = None
        self._candle_close   = None
        self._candle_minute  = None   # which minute this candle belongs to

        self._lock = threading.Lock()

    def seed_from_bars(self, highs: list, lows: list, closes: list):
        """
        Pre-populate candle history from bar data on startup.
        This means ATR is available immediately, not just after
        enough 1-minute candles have built up live.
        """
        with self._lock:
            self._candle_highs.extend(highs)
            self._candle_lows.extend(lows)
            self._candle_closes.extend(closes)

    def on_tick(self, price: float, ts: datetime) -> bool:
        """
        Process a new tick. Returns True if a candle just closed
        (meaning ATR should be recalculated by the caller).
        """
        minute = ts.replace(second=0, microsecond=0)
        candle_closed = False

        with self._lock:
            if self._candle_minute is None:
                # first tick ever — open first candle
                self._candle_minute = minute
                self._candle_open   = price
                self._candle_high   = price
                self._candle_low    = price
                self._candle_close  = price

            elif minute > self._candle_minute:
                # new minute — close the previous candle and open a new one
                self._candle_highs.append(self._candle_high)
                self._candle_lows.append(self._candle_low)
                self._candle_closes.append(self._candle_close)

                # open new candle
                self._candle_minute = minute
                self._candle_open   = price
                self._candle_high   = price
                self._candle_low    = price
                self._candle_close  = price

                candle_closed = True

            else:
                # same minute — update current candle
                if price > self._candle_high:
                    self._candle_high = price
                if price < self._candle_low:
                    self._candle_low = price
                self._candle_close = price

        return candle_closed

    def get_atr(self) -> float:
        """Calculate ATR from completed candles."""
        with self._lock:
            h = list(self._candle_highs)
            l = list(self._candle_lows)
            c = list(self._candle_closes)
        if len(c) < 2:
            return 1.0   # not enough candles yet
        return _atr(h, l, c, self.atr_period)

    def get_candles(self, n: int = 10) -> list:
        """
        Return the last N completed candles as a list of dicts.
        Each dict has keys: high, low, close.
        Used by the strategy for price action signal evaluation.
        Returns empty list if fewer than n candles have completed.
        """
        with self._lock:
            highs  = list(self._candle_highs)
            lows   = list(self._candle_lows)
            closes = list(self._candle_closes)

        count = min(len(highs), len(lows), len(closes))
        if count == 0:
            return []

        candles = [
            {"high": highs[i], "low": lows[i], "close": closes[i]}
            for i in range(count)
        ]
        # return the most recent n candles
        return candles[-n:] if len(candles) >= n else candles

    def candle_count(self) -> int:
        with self._lock:
            return len(self._candle_closes)


# ── Price cache entry ─────────────────────────────────────────────────────────

class SymbolCache:
    """
    Holds all indicator state for one symbol.

    EMA, RSI, VWAP: updated every tick (fast, no high/low needed)
    ATR: updated every minute when a new candle closes (needs real OHLC)
    High/Low: tracked from ticks for price action signal
    """

    def __init__(self, symbol: str):
        self.symbol     = symbol
        self.price      = None
        self.volume     = 0
        self.updated_at = None

        self.ema9       = None
        self.ema21      = None
        self.atr        = None
        self.rsi        = None
        self.vwap       = None
        self.high       = None    # intraday high
        self.low        = None    # intraday low
        self.prev_high  = None    # previous session high (for PA signal)
        self.prev_low   = None    # previous session low

        # tick-based buffers
        self._closes        = deque(maxlen=200)
        self._intra_prices  = deque(maxlen=5000)
        self._intra_volumes = deque(maxlen=5000)

        # candle builder for ATR
        self._candles = CandleBuilder(atr_period=ATR_PERIOD)

        self._lock = threading.Lock()

    def seed_from_bars(self, bars: list):
        """
        Seed all indicators from historical 5-min bar data on startup.
        Gives the bot valid indicators before the first tick arrives.
        """
        if not bars:
            log.warning(f"[STREAM] No bars to seed {self.symbol}")
            return

        closes = [b["c"] for b in bars]
        highs  = [b["h"] for b in bars]
        lows   = [b["l"] for b in bars]
        vols   = [b["v"] for b in bars]

        with self._lock:
            # seed tick buffers with bar closes
            self._closes.extend(closes)

            # seed indicators
            self.ema9  = _ema(closes, 9)
            self.ema21 = _ema(closes, 21)
            self.rsi   = _rsi(closes)
            self.vwap  = _vwap(closes, vols)
            self.price = closes[-1]

            # price action context from last 20 bars
            self.high      = max(highs[-20:])
            self.low       = min(lows[-20:])
            self.prev_high = max(highs[-21:-1]) if len(highs) > 20 else self.high
            self.prev_low  = min(lows[-21:-1])  if len(lows) > 20  else self.low

            # seed intraday VWAP with today's bars only
            today = datetime.utcnow().date()
            for b in bars:
                bar_time = b.get("t", "")
                if str(today) in str(bar_time):
                    self._intra_prices.append(b["c"])
                    self._intra_volumes.append(b["v"])

            # mark freshly seeded so staleness check passes immediately
            self.updated_at = datetime.utcnow()

        # seed candle builder with bar OHLC so ATR is available from minute 1
        self._candles.seed_from_bars(highs, lows, closes)

        # calculate initial ATR from seeded candles
        self.atr = self._candles.get_atr()

        log.info(
            f"[STREAM] {self.symbol} seeded | "
            f"price={self.price:.2f} EMA9={self.ema9:.2f} "
            f"EMA21={self.ema21:.2f} ATR={self.atr:.4f} RSI={self.rsi:.1f} "
            f"candles={self._candles.candle_count()}"
        )

    def on_tick(self, price: float, volume: int, ts: datetime):
        """
        Process one trade tick.
        - Updates EMA, RSI, VWAP every tick (fast)
        - Updates ATR only when a 1-minute candle closes (accurate)
        """
        with self._lock:
            self.price      = price
            self.volume     = volume
            self.updated_at = datetime.utcnow()

            # tick buffers
            self._closes.append(price)
            self._intra_prices.append(price)
            self._intra_volumes.append(volume if volume > 0 else 1)

            closes = list(self._closes)

            # EMA — every tick
            if len(closes) >= 9:
                self.ema9 = _ema(closes, 9)
            if len(closes) >= 21:
                self.ema21 = _ema(closes, 21)

            # RSI — every tick
            if len(closes) >= 15:
                self.rsi = _rsi(closes)

            # VWAP — every tick
            ip = list(self._intra_prices)
            iv = list(self._intra_volumes)
            if ip:
                self.vwap = _vwap(ip, iv)

            # intraday high/low tracking
            if self.high is None or price > self.high:
                self.high = price
            if self.low is None or price < self.low:
                self.low = price

        # candle builder is thread-safe internally — call outside main lock
        # to avoid nested locking. Returns True when a candle just closed.
        candle_closed = self._candles.on_tick(price, ts)

        if candle_closed:
            # new 1-min candle closed — recalculate ATR from real OHLC data
            new_atr = self._candles.get_atr()
            with self._lock:
                self.atr = new_atr
            log.debug(
                f"[STREAM] {self.symbol} candle closed — "
                f"ATR updated to {new_atr:.4f} "
                f"({self._candles.candle_count()} candles)"
            )

    def get_candles(self, n: int = 10) -> list:
        """
        Return the last N completed 1-minute candles.
        Each candle is a dict with keys: high, low, close.
        Delegates to the candle builder.
        """
        return self._candles.get_candles(n)

    def to_dict(self) -> dict:
        """Return a snapshot of current state for the strategy to read."""
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
        """
        Seed indicators from bars, then connect WebSocket in background.
        Blocks up to 20s for the stream to come live.
        Falls back to seeded data if connection is slow.
        """
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
        """Gracefully shut down the stream."""
        self._running = False
        if self._ws:
            self._ws.close()

    def get_price(self, symbol: str) -> dict:
        """
        Get current price and all indicators for a symbol.
        Returns None if symbol is not being tracked.
        """
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
        """
        Return the last N completed 1-minute candles for a symbol.
        Each candle is a dict with keys: high, low, close.
        Used by the strategy for price action signal evaluation.
        Returns empty list if symbol not tracked or no candles yet.
        """
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        if cache is None:
            return []
        return cache.get_candles(n)

    # ── Bar seeding ───────────────────────────────────────────

    def _seed_all(self):
        """Fetch historical bars for every symbol to seed indicators."""
        for symbol in self.symbols:
            try:
                bars = self._fetch_bars(symbol)
                self._cache[symbol].seed_from_bars(bars)
            except Exception as e:
                log.error(f"[STREAM] Failed to seed {symbol}: {e}")

    def _fetch_bars(self, symbol: str) -> list:
        """
        Fetch recent 5-minute bars from Alpaca REST API.
        5-min bars give realistic ATR — 1-min bars underestimate volatility.
        """
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
            timeout=10,
        )
        resp.raise_for_status()
        bars = resp.json().get("bars", [])
        log.info(f"[STREAM] Fetched {len(bars)} {BAR_TIMEFRAME} bars for {symbol}")
        return bars

    # ── WebSocket loop ────────────────────────────────────────

    def _run_loop(self):
        """Reconnect loop — keeps retrying if connection drops."""
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
        """Open a single WebSocket connection."""
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

        # ── connection confirmed ──────────────────────────────
        if t == "success" and msg.get("msg") == "connected":
            log.info("[STREAM] Connected to Alpaca stream")

        # ── authenticated — subscribe ─────────────────────────
        elif t == "success" and msg.get("msg") == "authenticated":
            log.info("[STREAM] Authenticated ✓ — subscribing")
            ws.send(json.dumps({
                "action": "subscribe",
                "trades": self.symbols,
                "quotes": [],
                "bars":   [],
            }))

        # ── subscription confirmed ────────────────────────────
        elif t == "subscription":
            trades = msg.get("trades", [])
            log.info(f"[STREAM] Subscribed ✓ — receiving trades for {trades}")
            self._subscribed = True
            self._ready.set()

        # ── error ─────────────────────────────────────────────
        elif t == "error":
            code = msg.get("code")
            err  = msg.get("msg", "unknown")
            log.error(f"[STREAM] Error {code}: {err}")
            if code == 406:
                # connection limit — old connection still alive on Alpaca's end
                # close this one and let the reconnect loop retry in 10s
                log.warning("[STREAM] Connection limit (406) — closing and retrying")
                ws.close()

        # ── trade tick ────────────────────────────────────────
        elif t == "t":
            symbol = msg.get("S", "").upper()
            price  = msg.get("p")
            size   = msg.get("s", 0)

            if symbol not in self._cache or not price:
                return

            # parse timestamp from tick for candle builder
            # Alpaca sends timestamps as ISO strings e.g. "2026-05-21T14:30:01Z"
            raw_ts = msg.get("t", "")
            try:
                ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                # convert to naive UTC for consistency
                ts = ts.replace(tzinfo=None)
            except Exception:
                ts = datetime.utcnow()

            self._cache[symbol].on_tick(float(price), int(size), ts)

        # ── anything else ─────────────────────────────────────
        else:
            if t is not None:
                log.debug(f"[STREAM] Unhandled message type '{t}': {msg}")

    def _on_error(self, ws, error):
        log.error(f"[STREAM] WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        log.warning(f"[STREAM] WebSocket closed — code={code} msg={msg}")
        self._ready.clear()
        self._subscribed = False
