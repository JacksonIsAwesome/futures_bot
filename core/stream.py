"""
core/stream.py — Real-time price stream via Alpaca WebSocket v4

v4 changes (5-min bars + richer indicators):
  - Signal evaluation now uses 5-MINUTE candles instead of 1-minute.
    CandleBuilder tracks both 1-min candles (for ATR/RSI accuracy) and
    groups them into 5-min candles for strategy use.
  - ADX (Average Directional Index) added — pure trend-strength filter.
    ADX < 20 means choppy/ranging market, signals should be ignored.
    Calculated from 5-min candles, period 10 (responsive for intraday).
  - Opening range tracked — high/low of first 15 minutes of session.
    Resets at 9:30 AM ET every day. Gives Claude and signals a key
    reference level that none of the other indicators provide.
  - VWAP slope added — is VWAP itself rising or falling over last 3 bars?
    Rising VWAP = institutions consistently buying. Independent from
    whether price is above/below VWAP.
  - Session relative volume — today's total volume vs average session
    volume. Low relative volume = low institutional participation = weaker
    signals. Updated every 5-min candle close.

v3 (RSI fix):
  RSI calculated from candle closes only, not tick prices.

v2:
  CandleBuilder, get_candle_volumes, get_elapsed_seconds, get_candle_minute,
  get_current_candle_volume.
"""

import json
import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import requests
import websocket
import pytz

import config

log = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")

# ── Constants ─────────────────────────────────────────────────────────────────

WS_URL          = "wss://stream.data.alpaca.markets/v2/iex"
BARS_URL        = "https://data.alpaca.markets/v2/stocks/{symbol}/bars"
STALE_SECONDS   = 120
RECONNECT_DELAY = 10
BAR_LIMIT       = 200
BAR_TIMEFRAME   = "5Min"   # seed from 5-min bars
CANDLE_MINUTES  = 1        # 1-min candles internally
ATR_PERIOD      = 14
RSI_PERIOD      = 14
ADX_PERIOD      = 10       # shorter period for intraday responsiveness
FIVE_MIN_BARS   = 5        # 1-min candles per 5-min bar
OPENING_RANGE_MINS = 15    # first 15 minutes = opening range


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
    """RSI from candle closes only — never tick prices."""
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


def _adx(highs: list, lows: list, closes: list, period: int = 10) -> float:
    """
    Average Directional Index — measures trend STRENGTH not direction.
    Returns 0-100. Above 20 = trending, below 20 = ranging/choppy.
    Uses Wilder's smoothing method.
    Requires at least period+1 bars.
    """
    if len(closes) < period + 2:
        return 0.0

    # Calculate +DM, -DM, TR for each bar
    plus_dm_list  = []
    minus_dm_list = []
    tr_list       = []

    for i in range(1, len(closes)):
        high_diff = highs[i] - highs[i - 1]
        low_diff  = lows[i - 1] - lows[i]

        plus_dm  = high_diff if high_diff > low_diff and high_diff > 0 else 0
        minus_dm = low_diff  if low_diff > high_diff and low_diff  > 0 else 0

        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1])
        )

        plus_dm_list.append(plus_dm)
        minus_dm_list.append(minus_dm)
        tr_list.append(tr)

    if len(tr_list) < period:
        return 0.0

    # Wilder smoothing for first period
    smooth_tr       = sum(tr_list[:period])
    smooth_plus_dm  = sum(plus_dm_list[:period])
    smooth_minus_dm = sum(minus_dm_list[:period])

    dx_list = []

    for i in range(period, len(tr_list)):
        smooth_tr       = smooth_tr       - (smooth_tr       / period) + tr_list[i]
        smooth_plus_dm  = smooth_plus_dm  - (smooth_plus_dm  / period) + plus_dm_list[i]
        smooth_minus_dm = smooth_minus_dm - (smooth_minus_dm / period) + minus_dm_list[i]

        if smooth_tr == 0:
            continue

        plus_di  = 100 * smooth_plus_dm  / smooth_tr
        minus_di = 100 * smooth_minus_dm / smooth_tr

        di_sum  = plus_di + minus_di
        di_diff = abs(plus_di - minus_di)

        dx = 100 * di_diff / di_sum if di_sum > 0 else 0
        dx_list.append(dx)

    if not dx_list:
        return 0.0

    # ADX = smoothed average of DX
    adx_val = sum(dx_list[-period:]) / min(len(dx_list), period)
    return round(adx_val, 2)


def _vwap(prices: list, volumes: list) -> float:
    if not prices or not volumes:
        return prices[-1] if prices else 0.0
    total_vol = sum(volumes)
    if total_vol == 0:
        return prices[-1]
    return sum(p * v for p, v in zip(prices, volumes)) / total_vol


def _vwap_slope(vwap_history: list) -> str:
    """
    Determine if VWAP is rising, falling, or flat over last 3 values.
    Returns 'up', 'down', or 'flat'.
    Rising VWAP = institutions consistently buying (bullish context).
    """
    if len(vwap_history) < 3:
        return "flat"
    recent = vwap_history[-3:]
    if recent[-1] > recent[0] * 1.00005:   # 0.005% threshold to avoid noise
        return "up"
    if recent[-1] < recent[0] * 0.99995:
        return "down"
    return "flat"


# ── 1-minute candle builder ───────────────────────────────────────────────────

class CandleBuilder:
    """
    Builds 1-minute candles from ticks.
    Groups every 5 completed 1-min candles into a 5-min candle.
    Updates ATR, RSI, ADX on every 5-min candle close.
    """

    def __init__(self, atr_period: int = ATR_PERIOD,
                 rsi_period: int = RSI_PERIOD,
                 adx_period: int = ADX_PERIOD):
        self.atr_period = atr_period
        self.rsi_period = rsi_period
        self.adx_period = adx_period

        maxlen = max(atr_period + rsi_period + adx_period + 20, 120)

        # 1-min candle history (for internal use)
        self._1min_highs   = deque(maxlen=maxlen)
        self._1min_lows    = deque(maxlen=maxlen)
        self._1min_closes  = deque(maxlen=maxlen)
        self._1min_volumes = deque(maxlen=maxlen)

        # 5-min candle history (for strategy signals)
        self._5min_highs   = deque(maxlen=maxlen)
        self._5min_lows    = deque(maxlen=maxlen)
        self._5min_closes  = deque(maxlen=maxlen)
        self._5min_volumes = deque(maxlen=maxlen)

        # In-progress 1-min candle
        self._cur_open   = None
        self._cur_high   = None
        self._cur_low    = None
        self._cur_close  = None
        self._cur_vol    = 0
        self._cur_minute = None
        self._cur_start  = None

        # Buffer for building 5-min candles from 1-min candles
        self._1min_buffer_h = []
        self._1min_buffer_l = []
        self._1min_buffer_c = []
        self._1min_buffer_v = []

        # Cached indicator values (updated on 5-min close)
        self._atr = 1.0
        self._rsi = 50.0
        self._adx = 0.0

        self._lock = threading.Lock()

    def seed_from_bars(self, highs: list, lows: list,
                       closes: list, volumes: list = None):
        """
        Seed from historical 5-min bars fetched at startup.
        Immediately computes ATR, RSI, ADX from seeded data.
        """
        with self._lock:
            self._5min_highs.extend(highs)
            self._5min_lows.extend(lows)
            self._5min_closes.extend(closes)
            if volumes:
                self._5min_volumes.extend(volumes)

            h = list(self._5min_highs)
            l = list(self._5min_lows)
            c = list(self._5min_closes)

            self._atr = _atr(h, l, c, self.atr_period)
            self._rsi = _rsi_from_closes(c, self.rsi_period)
            self._adx = _adx(h, l, c, self.adx_period)

    def on_tick(self, price: float, volume: int, ts: datetime) -> bool:
        """
        Process a tick. Returns True when a 5-min candle closes.
        """
        minute = ts.replace(second=0, microsecond=0)
        five_min_closed = False

        with self._lock:
            # ── Initialize first candle ───────────────────────
            if self._cur_minute is None:
                self._cur_minute = minute
                self._cur_start  = datetime.utcnow()
                self._cur_open   = price
                self._cur_high   = price
                self._cur_low    = price
                self._cur_close  = price
                self._cur_vol    = volume

            elif minute > self._cur_minute:
                # ── 1-min candle closed ───────────────────────
                self._1min_highs.append(self._cur_high)
                self._1min_lows.append(self._cur_low)
                self._1min_closes.append(self._cur_close)
                self._1min_volumes.append(self._cur_vol)

                # Buffer for 5-min candle
                self._1min_buffer_h.append(self._cur_high)
                self._1min_buffer_l.append(self._cur_low)
                self._1min_buffer_c.append(self._cur_close)
                self._1min_buffer_v.append(self._cur_vol)

                # Every 5 1-min candles = one 5-min candle
                if len(self._1min_buffer_h) >= FIVE_MIN_BARS:
                    five_h = max(self._1min_buffer_h)
                    five_l = min(self._1min_buffer_l)
                    five_c = self._1min_buffer_c[-1]
                    five_v = sum(self._1min_buffer_v)

                    self._5min_highs.append(five_h)
                    self._5min_lows.append(five_l)
                    self._5min_closes.append(five_c)
                    self._5min_volumes.append(five_v)

                    # Recalculate all indicators from 5-min candles
                    h = list(self._5min_highs)
                    l = list(self._5min_lows)
                    c = list(self._5min_closes)

                    self._atr = _atr(h, l, c, self.atr_period)
                    self._rsi = _rsi_from_closes(c, self.rsi_period)
                    self._adx = _adx(h, l, c, self.adx_period)

                    self._1min_buffer_h.clear()
                    self._1min_buffer_l.clear()
                    self._1min_buffer_c.clear()
                    self._1min_buffer_v.clear()

                    five_min_closed = True

                # Start new 1-min candle
                self._cur_minute = minute
                self._cur_start  = datetime.utcnow()
                self._cur_open   = price
                self._cur_high   = price
                self._cur_low    = price
                self._cur_close  = price
                self._cur_vol    = volume

            else:
                if price > self._cur_high: self._cur_high = price
                if price < self._cur_low:  self._cur_low  = price
                self._cur_close = price
                self._cur_vol  += volume

        return five_min_closed

    def get_atr(self) -> float:
        with self._lock: return self._atr

    def get_rsi(self) -> float:
        with self._lock: return self._rsi

    def get_adx(self) -> float:
        with self._lock: return self._adx

    def get_5min_candles(self, n: int = 15) -> list:
        """Returns last N completed 5-min candles for strategy use."""
        with self._lock:
            h = list(self._5min_highs)
            l = list(self._5min_lows)
            c = list(self._5min_closes)
            v = list(self._5min_volumes)

        count = min(len(h), len(l), len(c))
        if count == 0:
            return []
        candles = [
            {"high": h[i], "low": l[i], "close": c[i],
             "volume": v[i] if i < len(v) else 0}
            for i in range(count)
        ]
        return candles[-n:] if len(candles) >= n else candles

    def get_candle_volumes(self, n: int = 20) -> list:
        with self._lock:
            vols = list(self._5min_volumes)
        return vols[-n:] if len(vols) >= n else vols

    def get_elapsed_seconds(self) -> float:
        with self._lock:
            if self._cur_start is None:
                return 30.0
            return max((datetime.utcnow() - self._cur_start).total_seconds(), 1.0)

    def get_candle_minute(self):
        with self._lock: return self._cur_minute

    def get_current_candle_volume(self) -> int:
        with self._lock: return self._cur_vol

    def candle_count(self) -> int:
        with self._lock: return len(self._5min_closes)


# ── Opening range tracker ─────────────────────────────────────────────────────

class OpeningRangeTracker:
    """
    Tracks the opening range high/low (first 15 minutes of session).
    Resets at 9:30 AM ET every trading day.

    The opening range is one of the most statistically validated intraday
    levels — institutions use it as a reference and breakouts of it on
    volume are high-probability setups.
    """

    def __init__(self):
        self._range_high  = None
        self._range_low   = None
        self._range_set   = False
        self._last_date   = None
        self._lock        = threading.Lock()

    def on_tick(self, price: float, ts: datetime):
        """Feed price ticks during the opening range window."""
        now_et = ts.astimezone(ET) if ts.tzinfo else ET.localize(ts)
        today  = now_et.date()

        with self._lock:
            # Reset on new day
            if today != self._last_date:
                self._range_high = None
                self._range_low  = None
                self._range_set  = False
                self._last_date  = today

            if self._range_set:
                return

            # Only track during first OPENING_RANGE_MINS minutes after 9:30
            market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            range_end   = market_open + timedelta(minutes=OPENING_RANGE_MINS)

            if now_et < market_open:
                return

            if now_et <= range_end:
                if self._range_high is None or price > self._range_high:
                    self._range_high = price
                if self._range_low is None or price < self._range_low:
                    self._range_low = price
            else:
                # Range window closed — lock it in
                if self._range_high is not None:
                    self._range_set = True

    def get_range(self) -> tuple:
        """Returns (high, low) or (None, None) if not yet established."""
        with self._lock:
            return self._range_high, self._range_low

    def is_set(self) -> bool:
        with self._lock:
            return self._range_set


# ── Price cache entry ─────────────────────────────────────────────────────────

class SymbolCache:
    """
    Holds all indicator state for one symbol.

    Updated every tick: price, EMA, VWAP, volume
    Updated every 5-min candle close: ATR, RSI, ADX
    Updated continuously: opening range, VWAP slope, session volume
    """

    def __init__(self, symbol: str):
        self.symbol     = symbol
        self.price      = None
        self.volume     = 0
        self.updated_at = None

        self.ema9       = None
        self.ema21      = None
        self.atr        = None
        self.rsi        = 50.0
        self.adx        = 0.0
        self.vwap       = None
        self.vwap_slope = "flat"
        self.high       = None
        self.low        = None
        self.prev_high  = None
        self.prev_low   = None

        # Session volume tracking
        self.session_volume      = 0
        self.avg_session_volume  = 0   # will be estimated from seeded bars
        self.relative_volume     = 1.0

        self._closes        = deque(maxlen=300)
        self._intra_prices  = deque(maxlen=5000)
        self._intra_volumes = deque(maxlen=5000)
        self._vwap_history  = deque(maxlen=10)  # for slope calculation

        self._candles = CandleBuilder(
            atr_period=ATR_PERIOD,
            rsi_period=RSI_PERIOD,
            adx_period=ADX_PERIOD
        )
        self._opening_range = OpeningRangeTracker()

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
            session_vols = []
            for b in bars:
                bar_time = b.get("t", "")
                if str(today) in str(bar_time):
                    self._intra_prices.append(b["c"])
                    self._intra_volumes.append(b["v"])
                    session_vols.append(b["v"])
                    self.session_volume += b["v"]

            # Estimate average session volume from historical bars
            # Use total volume of all seeded bars / number of days represented
            # Rough estimate: seeded bars span ~2 trading days at 5-min
            if vols:
                self.avg_session_volume = sum(vols)  # will be refined over time
            self.relative_volume = 1.0

            self.updated_at = datetime.utcnow()

        # Seed candle builder with 5-min bars
        self._candles.seed_from_bars(highs, lows, closes, vols)
        self.atr = self._candles.get_atr()
        self.rsi = self._candles.get_rsi()
        self.adx = self._candles.get_adx()

        log.info(
            f"[STREAM] {self.symbol} seeded | "
            f"price={self.price:.2f} EMA9={self.ema9:.2f} "
            f"EMA21={self.ema21:.2f} ATR={self.atr:.4f} "
            f"RSI={self.rsi:.1f} ADX={self.adx:.1f} "
            f"candles={self._candles.candle_count()}"
        )

    def on_tick(self, price: float, volume: int, ts: datetime):
        # Feed opening range tracker
        self._opening_range.on_tick(price, ts)

        with self._lock:
            self.price      = price
            self.volume     = volume
            self.updated_at = datetime.utcnow()
            self.session_volume += volume

            self._closes.append(price)
            self._intra_prices.append(price)
            self._intra_volumes.append(volume if volume > 0 else 1)

            closes = list(self._closes)

            if len(closes) >= 9:
                self.ema9 = _ema(closes, 9)
            if len(closes) >= 21:
                self.ema21 = _ema(closes, 21)

            ip = list(self._intra_prices)
            iv = list(self._intra_volumes)
            if ip:
                new_vwap = _vwap(ip, iv)
                self.vwap = new_vwap
                self._vwap_history.append(new_vwap)
                self.vwap_slope = _vwap_slope(list(self._vwap_history))

            if self.high is None or price > self.high:
                self.high = price
            if self.low is None or price < self.low:
                self.low = price

        # Feed candle builder
        five_min_closed = self._candles.on_tick(price, volume, ts)

        if five_min_closed:
            new_atr = self._candles.get_atr()
            new_rsi = self._candles.get_rsi()
            new_adx = self._candles.get_adx()
            with self._lock:
                self.atr = new_atr
                self.rsi = new_rsi
                self.adx = new_adx

                # Update relative volume on 5-min candle close
                if self.avg_session_volume > 0:
                    self.relative_volume = round(
                        self.session_volume / self.avg_session_volume, 2
                    )

            log.debug(
                f"[STREAM] {self.symbol} 5-min candle — "
                f"ATR={new_atr:.4f} RSI={new_rsi:.1f} ADX={new_adx:.1f}"
            )

    def get_5min_candles(self, n: int = 15) -> list:
        return self._candles.get_5min_candles(n)

    def get_candle_volumes(self, n: int = 20) -> list:
        return self._candles.get_candle_volumes(n)

    def get_elapsed_seconds(self) -> float:
        return self._candles.get_elapsed_seconds()

    def get_candle_minute(self):
        return self._candles.get_candle_minute()

    def get_current_candle_volume(self) -> int:
        return self._candles.get_current_candle_volume()

    def get_opening_range(self) -> tuple:
        return self._opening_range.get_range()

    def to_dict(self) -> dict:
        with self._lock:
            stale = (
                self.updated_at is None or
                (datetime.utcnow() - self.updated_at).total_seconds() > STALE_SECONDS
            )
            or_high, or_low = self._opening_range.get_range()
            return {
                "price":           self.price,
                "volume":          self.volume,
                "ema9":            self.ema9,
                "ema21":           self.ema21,
                "atr":             self.atr,
                "rsi":             self.rsi,
                "adx":             self.adx,
                "vwap":            self.vwap,
                "vwap_slope":      self.vwap_slope,
                "high":            self.high,
                "low":             self.low,
                "prev_high":       self.prev_high,
                "prev_low":        self.prev_low,
                "opening_range_high": or_high,
                "opening_range_low":  or_low,
                "session_volume":     self.session_volume,
                "relative_volume":    self.relative_volume,
                "updated_at":      self.updated_at,
                "stale":           stale,
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
        return cache.to_dict() if cache else None

    def is_stale(self, symbol: str) -> bool:
        data = self.get_price(symbol)
        return data is None or data["stale"]

    def get_candles(self, symbol: str, n: int = 15) -> list:
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        return cache.get_5min_candles(n) if cache else []

    def get_candle_volumes(self, symbol: str, n: int = 20) -> list:
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        return cache.get_candle_volumes(n) if cache else []

    def get_elapsed_seconds(self, symbol: str) -> float:
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        return cache.get_elapsed_seconds() if cache else 30.0

    def get_candle_minute(self, symbol: str):
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        return cache.get_candle_minute() if cache else None

    def get_current_candle_volume(self, symbol: str) -> int:
        symbol = symbol.upper()
        cache  = self._cache.get(symbol)
        return cache.get_current_candle_volume() if cache else 0

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
            except Exception:
                ts = datetime.now(timezone.utc)

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
