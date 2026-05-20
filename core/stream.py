"""
core/stream.py — Real-time price stream via Alpaca WebSocket

Connects to Alpaca's free IEX WebSocket feed and maintains a live
price cache that the strategy reads from instead of polling bars.

On startup:
  - Fetches historical bars to seed EMA, VWAP, ATR calculations
  - Connects to WebSocket and subscribes to trades for all symbols
  - Updates price cache on every tick

The strategy reads from stream.get_price(symbol) instead of data.py.
If the stream is down or stale, falls back to the last known price
with a staleness flag so the bot can skip the symbol.

Usage in main.py:
    from core.stream import PriceStream
    stream = PriceStream(symbols=config.SYMBOLS)
    stream.start()   # non-blocking, runs in background thread
    ...
    cache = stream.get_price("NVDA")
    # cache = {
    #   "price": 224.46,
    #   "volume": 12345,
    #   "vwap": 224.10,
    #   "ema9": 223.80,
    #   "ema21": 223.50,
    #   "atr": 0.44,
    #   "rsi": 58.2,
    #   "high": 225.00,
    #   "low": 223.00,
    #   "prev_high": 224.50,
    #   "prev_low": 223.20,
    #   "updated_at": datetime(...),
    #   "stale": False
    # }
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

WS_URL        = "wss://stream.data.alpaca.markets/v2/iex"
BARS_URL      = "https://data.alpaca.markets/v2/stocks/{symbol}/bars"
STALE_SECONDS = 120   # mark stale if no tick for 2 minutes
RECONNECT_DELAY = 5   # seconds between reconnect attempts
BAR_LIMIT     = 50    # bars to fetch on startup for indicator seeding

# ── Indicator math ────────────────────────────────────────────────────────────

def _ema(prices: list, period: int) -> float:
    """Calculate EMA from a list of prices."""
    if len(prices) < period:
        return sum(prices) / len(prices)
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return ema


def _atr(highs: list, lows: list, closes: list, period: int = 14) -> float:
    """Calculate ATR from bar data."""
    if len(closes) < 2:
        return abs(highs[-1] - lows[-1]) if highs and lows else 0.5
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1])
        )
        trs.append(tr)
    period = min(period, len(trs))
    return sum(trs[-period:]) / period


def _rsi(closes: list, period: int = 14) -> float:
    """Calculate RSI from close prices."""
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
    """Calculate VWAP from intraday prices and volumes."""
    if not prices or not volumes:
        return prices[-1] if prices else 0
    total_vol = sum(volumes)
    if total_vol == 0:
        return prices[-1]
    return sum(p * v for p, v in zip(prices, volumes)) / total_vol


# ── Price cache entry ─────────────────────────────────────────────────────────

class SymbolCache:
    """Holds all indicator state for one symbol."""

    def __init__(self, symbol: str):
        self.symbol     = symbol
        self.price      = None
        self.volume     = 0
        self.updated_at = None

        # indicator values (seeded from bars, updated on ticks)
        self.ema9       = None
        self.ema21      = None
        self.atr        = None
        self.rsi        = None
        self.vwap       = None
        self.high       = None
        self.low        = None
        self.prev_high  = None
        self.prev_low   = None

        # rolling buffers for indicator updates
        self._closes    = deque(maxlen=50)
        self._highs     = deque(maxlen=50)
        self._lows      = deque(maxlen=50)
        self._intra_prices  = deque(maxlen=500)  # intraday for VWAP
        self._intra_volumes = deque(maxlen=500)

        self._lock = threading.Lock()

    def seed_from_bars(self, bars: list):
        """
        Seed indicators from historical bar data on startup.
        bars: list of dicts with keys: c (close), h (high), l (low), v (volume)
        """
        if not bars:
            log.warning(f"[STREAM] No bars to seed {self.symbol}")
            return

        closes = [b["c"] for b in bars]
        highs  = [b["h"] for b in bars]
        lows   = [b["l"] for b in bars]
        vols   = [b["v"] for b in bars]

        with self._lock:
            self._closes.extend(closes)
            self._highs.extend(highs)
            self._lows.extend(lows)

            self.ema9      = _ema(closes, 9)
            self.ema21     = _ema(closes, 21)
            self.atr       = _atr(highs, lows, closes)
            self.rsi       = _rsi(closes)
            self.vwap      = _vwap(closes, vols)
            self.price     = closes[-1]
            self.high      = max(highs[-20:])
            self.low       = min(lows[-20:])
            self.prev_high = max(highs[-21:-1]) if len(highs) > 20 else self.high
            self.prev_low  = min(lows[-21:-1])  if len(lows)  > 20 else self.low

            # seed intraday buffers
            today = datetime.utcnow().date()
            for b in bars:
                bar_time = b.get("t", "")
                if str(today) in str(bar_time):
                    self._intra_prices.append(b["c"])
                    self._intra_volumes.append(b["v"])

        log.info(
            f"[STREAM] {self.symbol} seeded | "
            f"price={self.price:.2f} EMA9={self.ema9:.2f} "
            f"EMA21={self.ema21:.2f} ATR={self.atr:.4f} RSI={self.rsi:.1f}"
        )

    def on_tick(self, price: float, volume: int = 0):
        """Update price and recalculate indicators on every trade tick."""
        with self._lock:
            self.price      = price
            self.volume     = volume
            self.updated_at = datetime.utcnow()

            # update close buffer
            self._closes.append(price)
            closes = list(self._closes)

            # update intraday VWAP buffers
            self._intra_prices.append(price)
            self._intra_volumes.append(volume if volume > 0 else 1)

            # recalculate EMAs on every tick
            if len(closes) >= 9:
                self.ema9 = _ema(closes, 9)
            if len(closes) >= 21:
                self.ema21 = _ema(closes, 21)

            # RSI updates every tick
            if len(closes) >= 15:
                self.rsi = _rsi(closes)

            # VWAP from intraday data
            ip = list(self._intra_prices)
            iv = list(self._intra_volumes)
            if ip:
                self.vwap = _vwap(ip, iv)

            # update high/low
            if self.high is None or price > self.high:
                self.high = price
            if self.low is None or price < self.low:
                self.low = price

    def to_dict(self) -> dict:
        """Return snapshot of current state for the strategy to read."""
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
    Manages the Alpaca WebSocket connection and price cache.
    Runs in a background thread — non-blocking for the main bot loop.
    """

    def __init__(self, symbols: list):
        self.symbols   = [s.upper() for s in symbols]
        self._cache    = {s: SymbolCache(s) for s in self.symbols}
        self._ws       = None
        self._running  = False
        self._thread   = None
        self._ready    = threading.Event()  # set when stream is live

    # ── Public API ────────────────────────────────────────────

    def start(self):
        """
        Seed indicators from bars, then start WebSocket in background.
        Blocks until the stream is authenticated and subscribed.
        """
        log.info(f"[STREAM] Starting for {self.symbols}")
        self._seed_all()
        self._running = True
        self._thread  = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        # wait up to 15s for connection
        if self._ready.wait(timeout=15):
            log.info("[STREAM] ✓ WebSocket live and subscribed")
        else:
            log.warning("[STREAM] WebSocket not ready after 15s — using seeded data")

    def stop(self):
        """Gracefully shut down the stream."""
        self._running = False
        if self._ws:
            self._ws.close()

    def get_price(self, symbol: str) -> dict:
        """
        Get current price and indicators for a symbol.
        Returns None if symbol not tracked.
        """
        symbol = symbol.upper()
        cache = self._cache.get(symbol)
        if cache is None:
            return None
        return cache.to_dict()

    def is_ready(self) -> bool:
        return self._ready.is_set()

    def is_stale(self, symbol: str) -> bool:
        data = self.get_price(symbol)
        return data is None or data["stale"]

    # ── Bar seeding ───────────────────────────────────────────

    def _seed_all(self):
        """Fetch historical bars for all symbols to seed indicators."""
        for symbol in self.symbols:
            try:
                bars = self._fetch_bars(symbol)
                self._cache[symbol].seed_from_bars(bars)
            except Exception as e:
                log.error(f"[STREAM] Failed to seed {symbol}: {e}")

    def _fetch_bars(self, symbol: str) -> list:
        """Fetch recent 1-minute bars from Alpaca REST API."""
        end   = datetime.utcnow()
        start = end - timedelta(hours=8)  # last 8 hours covers today + premarket

        resp = requests.get(
            BARS_URL.format(symbol=symbol),
            headers={
                "APCA-API-KEY-ID":     config.ALPACA_API_KEY,
                "APCA-API-SECRET-KEY": config.ALPACA_SECRET_KEY,
            },
            params={
                "timeframe": "1Min",
                "start":     start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end":       end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "limit":     BAR_LIMIT,
                "feed":      "iex",
            },
            timeout=10,
        )
        resp.raise_for_status()
        bars = resp.json().get("bars", [])
        log.info(f"[STREAM] Fetched {len(bars)} bars for {symbol}")
        return bars

    # ── WebSocket loop ────────────────────────────────────────

    def _run_loop(self):
        """Main reconnect loop. Keeps trying if connection drops."""
        while self._running:
            try:
                self._connect()
            except Exception as e:
                log.error(f"[STREAM] WebSocket error: {e}")
            if self._running:
                log.info(f"[STREAM] Reconnecting in {RECONNECT_DELAY}s...")
                self._ready.clear()
                time.sleep(RECONNECT_DELAY)

    def _connect(self):
        """Open one WebSocket connection."""
        log.info(f"[STREAM] Connecting to {WS_URL}")
        self._ws = websocket.WebSocketApp(
            WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws.run_forever(ping_interval=30, ping_timeout=10)

    # ── WebSocket handlers ────────────────────────────────────

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
        t = msg.get("T")  # message type

        if t == "connected":
            log.info("[STREAM] Connected to Alpaca stream")

        elif t == "success":
            log.info(f"[STREAM] Success message: {msg}")  # temp debug line
            if msg.get("msg") == "authenticated":
                log.info("[STREAM] Authenticated ✓ — subscribing")
                ws.send(json.dumps({
                    "action":  "subscribe",
                    "trades":  self.symbols,
                    "quotes":  [],
                    "bars":    [],
                }))
            elif msg.get("msg") == "subscribed" or msg.get("trades"):
                log.info(f"[STREAM] Subscribed to {self.symbols} ✓")
                self._ready.set()

        elif t == "error":
            code = msg.get("code")
            log.error(f"[STREAM] Error: {msg}")
            if code == 406:
                log.info("[STREAM] Connection limit — waiting 10s for old connection to clear...")
                time.sleep(10)
                ws.close()

        elif t == "t":
            # trade tick
            symbol = msg.get("S", "").upper()
            price  = msg.get("p")
            size   = msg.get("s", 0)
            if symbol in self._cache and price:
                self._cache[symbol].on_tick(float(price), int(size))

        # "q" = quote (bid/ask) — ignored for now, trades are enough

    def _on_error(self, ws, error):
        log.error(f"[STREAM] WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        log.warning(f"[STREAM] WebSocket closed — code={code} msg={msg}")
        self._ready.clear()
