"""
core/stream.py — Real-time price stream via Alpaca WebSocket

Connects to Alpaca's free IEX WebSocket feed and maintains a live
price cache that the strategy reads from instead of polling bars.

On startup:
  - Fetches historical 5-minute bars to seed EMA, VWAP, ATR calculations
    (5-min bars give better ATR than 1-min — captures real intraday volatility)
  - Connects to WebSocket and subscribes to trades for all symbols
  - Updates price cache on every tick

ATR is seeded from bars and NOT updated from ticks — tick-to-tick price
differences are too small to give a meaningful ATR. ATR stays fixed at
the seeded value until the next daily restart when new bars are fetched.

The strategy reads from stream.get_price(symbol) instead of data.py.
If the stream is down or stale, the cache still returns seeded data
with stale=True so the bot can skip new entries safely.
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
BAR_LIMIT       = 100   # more bars = better ATR calculation
BAR_TIMEFRAME   = "5Min"  # 5-min bars capture real intraday volatility

# ── Indicator math ────────────────────────────────────────────────────────────

def _ema(prices: list, period: int) -> float:
    if len(prices) < period:
        return sum(prices) / len(prices)
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return ema


def _atr(highs: list, lows: list, closes: list, period: int = 14) -> float:
    """
    Calculate ATR from OHLC bar data.
    This is the TRUE ATR using bar high/low ranges — much more accurate
    than tick-to-tick differences. Called once on startup from bar data.
    """
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

        self.ema9       = None
        self.ema21      = None
        self.atr        = None   # seeded from bars, NOT updated from ticks
        self.rsi        = None
        self.vwap       = None
        self.high       = None
        self.low        = None
        self.prev_high  = None
        self.prev_low   = None

        self._closes         = deque(maxlen=100)
        self._highs          = deque(maxlen=100)
        self._lows           = deque(maxlen=100)
        self._intra_prices   = deque(maxlen=500)
        self._intra_volumes  = deque(maxlen=500)

        self._lock = threading.Lock()

    def seed_from_bars(self, bars: list):
        """
        Seed all indicators from historical bar data on startup.
        bars: list of dicts with keys: c, h, l, o, v, t
        Uses 5-min bars for better ATR calculation.
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

            self.ema9  = _ema(closes, 9)
            self.ema21 = _ema(closes, 21)

            # ATR from true OHLC bar data — much more accurate than ticks
            # This is the key fix: using bar highs/lows gives real ATR,
            # not the tiny tick-to-tick differences
            self.atr = _atr(highs, lows, closes)

            self.rsi   = _rsi(closes)
            self.vwap  = _vwap(closes, vols)
            self.price = closes[-1]

            # use last 20 bars for high/low context
            self.high      = max(highs[-20:])
            self.low       = min(lows[-20:])
            self.prev_high = max(highs[-21:-1]) if len(highs) > 20 else self.high
            self.prev_low  = min(lows[-21:-1])  if len(lows)  > 20 else self.low

            # seed intraday VWAP with today's bars only
            today = datetime.utcnow().date()
            for b in bars:
                bar_time = b.get("t", "")
                if str(today) in str(bar_time):
                    self._intra_prices.append(b["c"])
                    self._intra_volumes.append(b["v"])

            # mark as freshly seeded so staleness check passes immediately
            self.updated_at = datetime.utcnow()

        log.info(
            f"[STREAM] {self.symbol} seeded | "
            f"price={self.price:.2f} EMA9={self.ema9:.2f} "
            f"EMA21={self.ema21:.2f} ATR={self.atr:.4f} RSI={self.rsi:.1f}"
        )

    def on_tick(self, price: float, volume: int = 0):
        """
        Update price and recalculate tick-based indicators on every trade.
        ATR is intentionally NOT updated here — it stays at the seeded
        value from bars which is far more accurate for stop/TP sizing.
        """
        with self._lock:
            self.price      = price
            self.volume     = volume
            self.updated_at = datetime.utcnow()

            # update close buffer for EMA/RSI
            self._closes.append(price)
            closes = list(self._closes)

            # update intraday VWAP
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

            # track intraday high/low
            if self.high is None or price > self.high:
                self.high = price
            if self.low is None or price < self.low:
                self.low = price

            # NOTE: ATR is NOT updated here — seeded value from bars is used

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

    # ── Bar seeding ───────────────────────────────────────────

    def _seed_all(self):
        for symbol in self.symbols:
            try:
                bars = self._fetch_bars(symbol)
                self._cache[symbol].seed_from_bars(bars)
            except Exception as e:
                log.error(f"[STREAM] Failed to seed {symbol}: {e}")

    def _fetch_bars(self, symbol: str) -> list:
        """
        Fetch recent bars from Alpaca REST API.
        Uses 5-minute bars for better ATR — 1-min bars give tiny
        high/low ranges that underestimate real volatility.
        """
        end   = datetime.utcnow()
        start = end - timedelta(hours=10)  # enough to get BAR_LIMIT 5-min bars

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
                log.warning("[STREAM] Connection limit (406) — closing and retrying in 10s")
                ws.close()

        elif t == "t":
            symbol = msg.get("S", "").upper()
            price  = msg.get("p")
            size   = msg.get("s", 0)
            if symbol in self._cache and price:
                self._cache[symbol].on_tick(float(price), int(size))

        else:
            if t not in (None,):
                log.debug(f"[STREAM] Unhandled message type '{t}': {msg}")

    def _on_error(self, ws, error):
        log.error(f"[STREAM] WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        log.warning(f"[STREAM] WebSocket closed — code={code} msg={msg}")
        self._ready.clear()
        self._subscribed = False
