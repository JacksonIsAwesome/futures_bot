"""
strategies/ema_vwap.py — EMA + VWAP Momentum Strategy (v2 — Anticipation Edition)

Evaluates 6 signals and scores them 0-6.
A trade fires when score >= MIN_SIGNAL_SCORE AND ROC is confirmed.

═══════════════════════════════════════════════════════════════
WHAT CHANGED FROM v1 AND WHY
═══════════════════════════════════════════════════════════════

v1 Problem: All 5 signals were lagging. By requiring 4/5 to fire,
the bot only entered after a move was already 60-70% done.

v2 Fixes:
  1. ROC (Rate of Change) — LEADING signal
     Measures price acceleration over the last 3 candles as a %.
     Fires at the *start* of a move, not after confirmation.
     This is what catches the SOXL open at 9:37am instead of 9:39am.
     ROC is REQUIRED — a trade cannot fire without it.

  2. VWAP Standard Deviation Bands — replaces old PA signal
     Instead of waiting 3 candles to form HH/HL structure, we measure
     how far price has deviated from VWAP in standard deviations.
     When price breaks 1.5+ std devs from VWAP with momentum, that
     is an early institutional-grade breakout signal that fires on
     the current bar, not after 3 bars close.

  3. Volume Acceleration — replaces raw volume spike check
     Old: was current candle volume > 1.4x rolling average (fires late)
     New: tracks volume *rate* within the current live candle.
     Compares volume accumulated so far this minute vs the expected
     pace from the rolling average. If we are on pace for 2x+ average
     volume with 30s left in the candle, that is the signal — not
     waiting for the candle to close.

  4. ROC required + lower score threshold (3/6 → fires earlier)
     With 3 leading/semi-leading signals replacing 2 lagging ones,
     a score of 3 with ROC confirmed is meaningful and early.
     MIN_SIGNAL_SCORE in config still controls this.

Signals (6 total):
  1. EMA trend      — EMA9 above/below EMA21 (lagging, directional filter)
  2. VWAP side      — price above/below VWAP (semi-leading)
  3. Vol accel      — volume accelerating intra-candle (LEADING)
  4. RSI confirm    — RSI not extreme (filter, prevents chasing)
  5. VWAP deviation — price > 1.5 std devs from VWAP (LEADING)
  6. ROC            — price accelerating (LEADING, REQUIRED)
"""

import math
import logging
from dataclasses import dataclass
from typing import Optional, List
from collections import deque
from core.database import get_config_override, log_signal
from meta.symbol_profiler import SymbolProfiler

_profiler = SymbolProfiler()
import config

log = logging.getLogger(__name__)

# ── ROC calculation ───────────────────────────────────────────────────────────

def _calc_roc(candles: list, period: int = 3) -> float:
    """
    Rate of Change: percentage price change over the last N candles.

    Uses candle closes so it's based on completed price data, not a
    single tick. Period=3 means we compare current close to 3 candles ago.

    Returns 0.0 if not enough candles.

    Positive = price accelerating up (bullish)
    Negative = price accelerating down (bearish)

    Example: close 3 bars ago = 100, current close = 101.5
    ROC = (101.5 - 100) / 100 * 100 = +1.5%
    """
    if not candles or len(candles) < period + 1:
        return 0.0
    current = candles[-1]["close"]
    past    = candles[-(period + 1)]["close"]
    if past == 0:
        return 0.0
    return (current - past) / past * 100.0


# ── VWAP standard deviation bands ────────────────────────────────────────────

def _calc_vwap_std(candles: list, vwap: float, lookback: int = 20) -> float:
    """
    Standard deviation of candle closes relative to VWAP over lookback period.

    This tells us how far price typically strays from VWAP in normal conditions.
    When price deviates more than 1.5x this amount, it signals a real breakout —
    not just noise.

    Returns the std dev value (in price units, same as ATR).
    Returns 0.0 if not enough data.
    """
    if not candles or len(candles) < 5:
        return 0.0
    recent_closes = [c["close"] for c in candles[-lookback:]]
    if len(recent_closes) < 3:
        return 0.0
    mean = sum(recent_closes) / len(recent_closes)
    variance = sum((c - mean) ** 2 for c in recent_closes) / len(recent_closes)
    return math.sqrt(variance)


def _check_vwap_deviation(candles: list, price: float, vwap: float,
                           direction: str, std_mult: float = 1.5) -> bool:
    """
    Returns True if price has broken significantly away from VWAP.

    For longs:  price > vwap + (std_mult * std_dev)  → bullish breakout
    For shorts: price < vwap - (std_mult * std_dev)  → bearish breakdown

    The std_mult (default 1.5) controls how far from VWAP we need to be.
    At 1.5 std devs, roughly 87% of normal price action is filtered out —
    only real moves trigger this.

    Falls back gracefully to a simple VWAP cross if std dev can't be calculated.
    """
    std = _calc_vwap_std(candles, vwap)

    if std == 0.0:
        # not enough candle data — fall back to simple VWAP side check
        return (price > vwap) if direction == "long" else (price < vwap)

    deviation = abs(price - vwap)
    threshold = std_mult * std

    if direction == "long":
        return price > vwap and deviation >= threshold
    else:
        return price < vwap and deviation >= threshold


# ── Volume acceleration ───────────────────────────────────────────────────────

class VolumeAccelerationTracker:
    """
    Tracks intra-candle volume acceleration per symbol.

    The old approach: compare completed candle volume to a rolling average.
    Problem: you have to wait for the candle to close to know its total volume.

    New approach: track how much volume has accumulated so far *this minute*
    and compare it to the expected pace from recent averages.

    If we are 30 seconds into a minute and already have 80% of a typical
    full-minute's volume, we are on pace for 2x+ — fire the signal NOW,
    not after the candle closes.

    Architecture:
    - Maintains a rolling window of completed candle volumes per symbol
    - Tracks current candle volume and start time
    - Every tick updates current candle volume
    - evaluate() computes projected_volume = current_vol * (60 / elapsed_secs)
    - Returns True if projected_volume >= required_mult * avg_volume
    """

    def __init__(self, window: int = 20):
        self._window = window
        # symbol -> deque of completed candle volumes
        self._history: dict = {}
        # symbol -> (current_candle_start_minute, accumulated_volume)
        self._current: dict = {}

    def on_tick(self, symbol: str, volume: int, candle_minute):
        """
        Called every tick with the symbol, tick size, and current minute.
        candle_minute: datetime truncated to the minute (candle boundary).
        """
        if symbol not in self._history:
            self._history[symbol] = deque(maxlen=self._window)
            self._current[symbol] = (candle_minute, 0)

        cur_minute, cur_vol = self._current[symbol]

        if candle_minute > cur_minute:
            # new candle started — archive the completed candle's volume
            if cur_vol > 0:
                self._history[symbol].append(cur_vol)
            # reset for new candle
            self._current[symbol] = (candle_minute, volume)
        else:
            # same candle — accumulate
            self._current[symbol] = (candle_minute, cur_vol + volume)

    def seed_from_candle_volumes(self, symbol: str, volumes: list):
        """Seed history from completed candle volumes on startup."""
        if symbol not in self._history:
            self._history[symbol] = deque(maxlen=self._window)
        for v in volumes[-self._window:]:
            self._history[symbol].append(v)

    def get_avg_volume(self, symbol: str) -> float:
        """Average volume per completed candle."""
        hist = self._history.get(symbol, deque())
        if len(hist) < 3:
            return 0.0
        return sum(hist) / len(hist)

    def get_volume_ratio(self, symbol: str, elapsed_seconds: float) -> float:
        """
        Returns projected_volume / avg_volume.

        projected_volume = current_candle_volume * (60 / elapsed_seconds)

        This projects what the candle's total volume will be if the
        current rate of volume continues for the rest of the minute.

        Example:
          avg_volume = 10,000 shares/candle
          elapsed = 15 seconds (quarter of the candle)
          current_vol = 6,000 shares so far
          projected = 6,000 * (60/15) = 24,000
          ratio = 24,000 / 10,000 = 2.4x — strong signal!
        """
        avg = self.get_avg_volume(symbol)
        if avg == 0:
            return 1.0

        hist = self._history.get(symbol, deque())
        if len(hist) < 3:
            return 1.0

        _, cur_vol = self._current.get(symbol, (None, 0))

        if elapsed_seconds < 3:
            # too early in candle — not enough data to project reliably
            return 1.0

        # project to full minute
        projected = cur_vol * (60.0 / elapsed_seconds)
        return projected / avg


# ── Module-level volume tracker (shared across all strategy evaluations) ──────
# This needs to survive across evaluate() calls for the same symbol.
_vol_tracker = VolumeAccelerationTracker(window=20)


# ── Signal dataclass ──────────────────────────────────────────────────────────

@dataclass
class Signal:
    symbol:         str
    direction:      Optional[str]
    score:          int
    price:          float
    atr:            float
    stop_loss:      float
    take_profit:    float
    ema_cross:      bool
    vwap_side:      bool
    vol_accel:      bool
    rsi_confirm:    bool
    vwap_deviation: bool
    roc_confirm:    bool
    roc_value:      float   # raw ROC % for logging/debugging
    signal_id:      Optional[str] = None

    def __str__(self):
        checks = [
            f"EMA={'✓' if self.ema_cross else '✗'}",
            f"VWAP={'✓' if self.vwap_side else '✗'}",
            f"VOL={'✓' if self.vol_accel else '✗'}",
            f"RSI={'✓' if self.rsi_confirm else '✗'}",
            f"DEV={'✓' if self.vwap_deviation else '✗'}",
            f"ROC={'✓' if self.roc_confirm else '✗'}({self.roc_value:+.2f}%)",
        ]
        return (
            f"[{self.symbol}] {self.direction or 'NONE'} "
            f"score={self.score}/6 | {' '.join(checks)} "
            f"| price={self.price:.2f} SL={self.stop_loss:.2f} "
            f"TP={self.take_profit:.2f}"
        )


# ── Main strategy class ───────────────────────────────────────────────────────

class EMAVWAPStrategy:
    """
    EMA + VWAP momentum strategy with anticipation-based signals.

    6 signals scored 0-6. Trade fires when:
      1. score >= MIN_SIGNAL_SCORE (configurable, default 4)
      2. ROC is confirmed (required — ensures we are in an accelerating move)

    The ROC requirement means the bot CANNOT enter a stale, slow-moving
    market even if all other signals align. This filters the choppy midday
    whipsaw that cost money in v1.
    """

    def __init__(self):
        log.info("[STRAT] EMA/VWAP strategy loaded ✓ (v2 — anticipation)")

    def _get_threshold(self, key, default):
        return get_config_override(key, default)

    def _val(self, sym_profile, profile_key, override_key, default):
        """Helper: profile value → DB override → config default."""
        if sym_profile and sym_profile.get(profile_key) is not None:
            return float(sym_profile[profile_key])
        return float(self._get_threshold(override_key, default))

    def seed_volume_history(self, symbol: str, candle_volumes: list):
        """
        Called once on startup with completed candle volumes for a symbol.
        Seeds the volume acceleration tracker so it has a baseline immediately.
        """
        _vol_tracker.seed_from_candle_volumes(symbol, candle_volumes)

    def on_tick(self, symbol: str, volume: int, candle_minute):
        """
        Called by main loop on every price tick.
        Feeds the volume acceleration tracker.

        Args:
            symbol:        ticker
            volume:        tick size (shares in this trade)
            candle_minute: datetime truncated to minute boundary
        """
        _vol_tracker.on_tick(symbol, volume, candle_minute)

    def evaluate(self, symbol: str, cache: dict,
                 candles: list = None,
                 elapsed_seconds: float = 30.0) -> Optional[Signal]:
        """
        Evaluate current stream data and return a Signal.

        Args:
            symbol:          ticker symbol
            cache:           dict from PriceStream.get_price(symbol)
            candles:         list of completed 1-min candle dicts from
                             PriceStream.get_candles(symbol).
                             Each candle has keys: high, low, close.
            elapsed_seconds: seconds elapsed since current candle opened.
                             Used for volume acceleration projection.
                             Main loop should pass this in from the stream.

        Returns Signal (direction=None if score < threshold) or None if
        data is insufficient.
        """
        # ── Guard: need valid live data ───────────────────────
        if cache is None:
            log.debug(f"[STRAT] {symbol}: no cache")
            return None

        if cache.get("stale"):
            log.warning(f"[STRAT] {symbol}: stream stale — skipping")
            return None

        price  = cache.get("price")
        ema9   = cache.get("ema9")
        ema21  = cache.get("ema21")
        atr    = cache.get("atr")
        rsi    = cache.get("rsi")
        vwap   = cache.get("vwap")
        volume = cache.get("volume", 0)

        if any(v is None for v in [price, ema9, ema21, atr, rsi, vwap]):
            log.debug(f"[STRAT] {symbol}: incomplete indicators")
            return None

        if atr == 0:
            log.debug(f"[STRAT] {symbol}: ATR is 0 — skipping")
            return None

        # ── Load symbol profile ───────────────────────────────
        sym_profile = _profiler.get_profile(symbol)

        min_score    = int(self._get_threshold("MIN_SIGNAL_SCORE", config.MIN_SIGNAL_SCORE))
        rsi_ob       = float(self._get_threshold("RSI_OVERBOUGHT", config.RSI_OVERBOUGHT))
        rsi_os       = float(self._get_threshold("RSI_OVERSOLD",   config.RSI_OVERSOLD))
        stop_mult    = self._val(sym_profile, "atr_stop_mult",  "ATR_STOP_MULT",       config.ATR_STOP_MULT)
        tp_mult      = self._val(sym_profile, "atr_tp_mult",    "ATR_TP_MULT",         config.ATR_TP_MULT)

        # ROC thresholds — how fast does price need to be moving?
        # Default: 0.08% per 3 candles for liquid ETFs. Symbol profile can tune.
        roc_period   = int(self._get_threshold("ROC_PERIOD",    getattr(config, "ROC_PERIOD",    3)))
        roc_min_long = float(self._get_threshold("ROC_MIN_LONG", getattr(config, "ROC_MIN_LONG", 0.08)))
        roc_min_short = float(self._get_threshold("ROC_MIN_SHORT", getattr(config, "ROC_MIN_SHORT", -0.08)))

        # VWAP deviation std multiplier — how far from VWAP is "real" breakout?
        vwap_dev_mult = float(self._get_threshold("VWAP_DEV_MULT", getattr(config, "VWAP_DEV_MULT", 1.5)))

        # Volume acceleration multiplier
        vol_accel_mult = float(self._get_threshold("VOL_ACCEL_MULT", getattr(config, "VOL_ACCEL_MULT", 1.8)))

        # ATR floor
        min_atr_pct   = float(sym_profile["min_atr_pct"])   if sym_profile and sym_profile.get("min_atr_pct")   else 0.003
        min_atr_floor = float(sym_profile["min_atr_floor"]) if sym_profile and sym_profile.get("min_atr_floor") else price * min_atr_pct
        atr = max(atr, min_atr_floor)

        # ── Direction from EMA relationship ───────────────────
        if ema9 > ema21:
            direction = "long"
        elif ema9 < ema21:
            direction = "short"
        else:
            return None

        candles = candles or []

        # ══════════════════════════════════════════════════════
        # SIGNAL 1: EMA Trend (lagging — directional filter)
        # ══════════════════════════════════════════════════════
        # EMA9 above EMA21 = uptrend. This is the baseline direction filter.
        # It's lagging but we keep it because it prevents counter-trend entries.
        ema_ok = (ema9 > ema21) if direction == "long" else (ema9 < ema21)

        # ══════════════════════════════════════════════════════
        # SIGNAL 2: VWAP Side (semi-leading)
        # ══════════════════════════════════════════════════════
        # Price above VWAP = institutional buying, below = selling.
        # Updates every tick so it's reasonably fast.
        if direction == "long":
            vwap_ok = price > vwap
        else:
            vwap_ok = price < vwap

        # ══════════════════════════════════════════════════════
        # SIGNAL 3: Volume Acceleration (LEADING — replaces old spike check)
        # ══════════════════════════════════════════════════════
        # Projects current candle's total volume based on pace so far.
        # If we are on pace for vol_accel_mult x average, signal fires NOW
        # instead of after the candle closes.
        vol_ratio = _vol_tracker.get_volume_ratio(symbol, elapsed_seconds)
        vol_ok    = vol_ratio >= vol_accel_mult

        # ══════════════════════════════════════════════════════
        # SIGNAL 4: RSI Confirmation (filter — prevents chasing extremes)
        # ══════════════════════════════════════════════════════
        # Not a leading signal — just prevents entering when RSI is already
        # at extreme levels where mean reversion is likely.
        if direction == "long":
            rsi_ok = rsi < rsi_ob   # not overbought
        else:
            rsi_ok = rsi > rsi_os   # not oversold

        # ══════════════════════════════════════════════════════
        # SIGNAL 5: VWAP Standard Deviation Deviation (LEADING)
        # Replaces old price action (3-candle HH/HL) signal
        # ══════════════════════════════════════════════════════
        # Measures how far price has moved from VWAP in standard deviation
        # units. When price breaks 1.5+ std devs, it signals institutional
        # participation — the kind of move that sustains.
        # Fires on current bar, not after 3 candles complete.
        dev_ok = _check_vwap_deviation(candles, price, vwap, direction, vwap_dev_mult)

        # ══════════════════════════════════════════════════════
        # SIGNAL 6: Rate of Change / Price Acceleration (LEADING — REQUIRED)
        # ══════════════════════════════════════════════════════
        # Measures how fast price is moving over the last roc_period candles.
        # This is the core anticipation signal — it fires at the START of moves.
        # A trade CANNOT fire without ROC confirming. This is the gate that
        # prevents entering slow, choppy, mean-reverting markets.
        roc_value = _calc_roc(candles, period=roc_period)

        if direction == "long":
            roc_ok = roc_value >= roc_min_long
        else:
            roc_ok = roc_value <= roc_min_short

        # ── Score ─────────────────────────────────────────────
        score = sum([ema_ok, vwap_ok, vol_ok, rsi_ok, dev_ok, roc_ok])

        # ── Calculate stops and take profit ───────────────────
        if direction == "long":
            stop_loss   = price - (atr * stop_mult)
            take_profit = price + (atr * tp_mult)
        else:
            stop_loss   = price + (atr * stop_mult)
            take_profit = price - (atr * tp_mult)

        # ── ROC is required — even if score passes, no ROC = no trade ─────
        # Score can be 6/6 but if ROC isn't confirming acceleration, we pass.
        # This is the key filter against choppy midday entries.
        direction_active = direction if (score >= min_score and roc_ok) else None

        # ── Log to DB for meta brain ──────────────────────────
        sig_id = log_signal(
            symbol=symbol,
            score=score,
            direction=direction_active,
            ema_cross=ema_ok,
            vwap_side=vwap_ok,
            volume_spike=vol_ok,
            rsi_confirm=rsi_ok,
            price_action=dev_ok,   # reusing price_action column for vwap deviation
            price=price,
            atr=atr
        )

        signal = Signal(
            symbol=symbol,
            direction=direction_active,
            score=score,
            price=price,
            atr=atr,
            stop_loss=round(stop_loss, 4),
            take_profit=round(take_profit, 4),
            ema_cross=ema_ok,
            vwap_side=vwap_ok,
            vol_accel=vol_ok,
            rsi_confirm=rsi_ok,
            vwap_deviation=dev_ok,
            roc_confirm=roc_ok,
            roc_value=roc_value,
            signal_id=sig_id
        )

        log.info(f"[STRAT] {signal}")
        return signal
