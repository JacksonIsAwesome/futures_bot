"""
strategies/ema_vwap.py — EMA + VWAP Momentum Strategy (v3)

Two-layer scoring gate:
  Layer 1 — Base signals (5): need BASE_MIN to enter
    1. EMA trend      — direction filter
    2. VWAP side      — institutional positioning
    3. Vol accel      — intra-candle volume projection
    4. RSI confirm    — not overextended
    5. VWAP deviation — breakout strength
  
  Layer 2 — Momentum confirmators (3): need MOMENTUM_GATE_MIN to enter
    6. ROC            — price acceleration (speed)
    7. MACD           — momentum building beneath surface (build)
    8. Candle consistency — sustained directional pressure (persistence)

  Plus:
    - Multi-timeframe filter: 5-min trend must agree (configurable)
    - Session-aware base score: lower bar in prime time (9:30-11am)
"""

import math
import logging
import pytz
from dataclasses import dataclass, field
from typing import Optional
from collections import deque
from datetime import datetime
from core.database import get_config_override, log_signal
from meta.symbol_profiler import SymbolProfiler

_profiler = SymbolProfiler()
import config

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")


# ── EMA helpers ───────────────────────────────────────────────────────────────

def _ema(prices: list, period: int) -> float:
    if not prices: return 0.0
    if len(prices) < period: return sum(prices) / len(prices)
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return ema


def _ema_list(prices: list, period: int) -> list:
    """Returns list of EMA values starting from index (period-1)."""
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    result = [ema]
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
        result.append(ema)
    return result


# ── ROC ───────────────────────────────────────────────────────────────────────

def _calc_roc(candles: list, period: int = 3) -> float:
    if not candles or len(candles) < period + 1:
        return 0.0
    current = candles[-1]["close"]
    past    = candles[-(period + 1)]["close"]
    if past == 0: return 0.0
    return (current - past) / past * 100.0


# ── MACD ──────────────────────────────────────────────────────────────────────

def _calc_macd(candles: list, fast: int = 12, slow: int = 26,
               signal_period: int = 9) -> tuple:
    """
    MACD histogram confirmation.

    Measures momentum building beneath the surface — fires when momentum
    is accelerating in the signal direction, before price has fully moved.

    long_ok:  histogram positive AND growing  (bullish momentum building)
    short_ok: histogram negative AND falling  (bearish momentum building)

    Returns (long_ok, short_ok, histogram_value)
    Minimum candles needed: slow + signal_period + 2
    """
    min_candles = slow + signal_period + 2
    if len(candles) < min_candles:
        return False, False, 0.0

    closes = [c["close"] for c in candles]

    ema_fast = _ema_list(closes, fast)
    ema_slow = _ema_list(closes, slow)

    if not ema_fast or not ema_slow:
        return False, False, 0.0

    # Align: ema_slow is shorter, offset ema_fast to match
    # ema_fast[i + offset] and ema_slow[i] both correspond to closes[slow-1+i]
    offset = slow - fast
    if offset < 0 or offset >= len(ema_fast):
        return False, False, 0.0

    macd_line = [ema_fast[i + offset] - ema_slow[i]
                 for i in range(len(ema_slow))]

    if len(macd_line) < signal_period + 1:
        return False, False, 0.0

    sig_line = _ema_list(macd_line, signal_period)
    if len(sig_line) < 2:
        return False, False, 0.0

    hist_curr = macd_line[-1] - sig_line[-1]
    hist_prev = macd_line[-2] - sig_line[-2]

    long_ok  = hist_curr > 0 and hist_curr > hist_prev
    short_ok = hist_curr < 0 and hist_curr < hist_prev

    return long_ok, short_ok, hist_curr


# ── Candle Consistency ────────────────────────────────────────────────────────

def _calc_candle_consistency(candles: list, lookback: int = 3,
                              min_consistent: int = 2,
                              direction: str = "long") -> bool:
    """
    Counts how many of the last N candles moved in the signal direction.
    Uses close-to-close comparison (no open data needed).

    Catches sustained pressure that ROC and MACD miss — 3 small consistent
    candles in one direction is more reliable than 1 big spike.
    """
    needed = lookback + 1  # +1 for comparison baseline
    if len(candles) < needed:
        return False

    recent = candles[-needed:]
    count  = 0
    for i in range(1, len(recent)):
        if direction == "long"  and recent[i]["close"] > recent[i-1]["close"]:
            count += 1
        elif direction == "short" and recent[i]["close"] < recent[i-1]["close"]:
            count += 1

    return count >= min_consistent


# ── Multi-timeframe filter ────────────────────────────────────────────────────

def _check_mtf_filter(candles: list, direction: str,
                       ema_period: int = 21) -> bool:
    """
    Multi-timeframe filter using a slow EMA on 1-min candles.
    EMA21 on 1-min ≈ 21-minute trend direction.

    Prevents entering against the broader trend even when 1-min signals align.
    Returns True (pass) if trend agrees with direction or not enough data.
    """
    if len(candles) < ema_period + 2:
        return True  # not enough data — don't block

    closes   = [c["close"] for c in candles]
    ema_vals = _ema_list(closes, ema_period)

    if len(ema_vals) < 2:
        return True

    trend_up = ema_vals[-1] > ema_vals[-2]

    if direction == "long":
        return trend_up
    else:
        return not trend_up


# ── VWAP standard deviation ───────────────────────────────────────────────────

def _calc_vwap_std(candles: list, vwap: float, lookback: int = 20) -> float:
    if not candles or len(candles) < 5:
        return 0.0
    recent = [c["close"] for c in candles[-lookback:]]
    if len(recent) < 3:
        return 0.0
    mean     = sum(recent) / len(recent)
    variance = sum((c - mean) ** 2 for c in recent) / len(recent)
    return math.sqrt(variance)


def _check_vwap_deviation(candles: list, price: float, vwap: float,
                           direction: str, std_mult: float = 1.5) -> bool:
    std = _calc_vwap_std(candles, vwap)
    if std == 0.0:
        return (price > vwap) if direction == "long" else (price < vwap)
    deviation = abs(price - vwap)
    threshold = std_mult * std
    if direction == "long":
        return price > vwap and deviation >= threshold
    else:
        return price < vwap and deviation >= threshold


# ── Volume acceleration ───────────────────────────────────────────────────────

class VolumeAccelerationTracker:
    def __init__(self, window: int = 20):
        self._window  = window
        self._history: dict = {}
        self._current: dict = {}

    def on_tick(self, symbol: str, volume: int, candle_minute):
        if symbol not in self._history:
            self._history[symbol] = deque(maxlen=self._window)
        if symbol not in self._current:
            self._current[symbol] = (candle_minute, 0)

        cur_minute, cur_vol = self._current[symbol]

        if candle_minute is None or cur_minute is None:
            self._current[symbol] = (candle_minute, cur_vol + volume)
            return

        if candle_minute > cur_minute:
            if cur_vol > 0:
                self._history[symbol].append(cur_vol)
            self._current[symbol] = (candle_minute, volume)
        else:
            self._current[symbol] = (candle_minute, cur_vol + volume)

    def seed_from_candle_volumes(self, symbol: str, volumes: list):
        if symbol not in self._history:
            self._history[symbol] = deque(maxlen=self._window)
        for v in volumes[-self._window:]:
            self._history[symbol].append(v)

    def get_avg_volume(self, symbol: str) -> float:
        hist = self._history.get(symbol, deque())
        if len(hist) < 3: return 0.0
        return sum(hist) / len(hist)

    def get_volume_ratio(self, symbol: str, elapsed_seconds: float) -> float:
        avg = self.get_avg_volume(symbol)
        if avg == 0: return 1.0
        hist = self._history.get(symbol, deque())
        if len(hist) < 3: return 1.0
        _, cur_vol = self._current.get(symbol, (None, 0))
        if elapsed_seconds < 3: return 1.0
        projected = cur_vol * (60.0 / elapsed_seconds)
        return projected / avg


_vol_tracker = VolumeAccelerationTracker(window=20)


# ── Signal dataclass ──────────────────────────────────────────────────────────

@dataclass
class Signal:
    symbol:         str
    direction:      Optional[str]
    score:          int          # base score (0-5)
    momentum_score: int          # momentum confirmations (0-3)
    price:          float
    atr:            float
    stop_loss:      float
    take_profit:    float
    # Base signals
    ema_cross:      bool
    vwap_side:      bool
    vol_accel:      bool
    rsi_confirm:    bool
    vwap_deviation: bool
    # Momentum signals
    roc_confirm:    bool
    macd_confirm:   bool
    candle_confirm: bool
    # MTF
    mtf_ok:         bool
    # Values for logging
    roc_value:      float
    macd_histogram: float
    signal_id:      Optional[str] = None

    def __str__(self):
        base = [
            f"EMA={'✓' if self.ema_cross else '✗'}",
            f"VWAP={'✓' if self.vwap_side else '✗'}",
            f"VOL={'✓' if self.vol_accel else '✗'}",
            f"RSI={'✓' if self.rsi_confirm else '✗'}",
            f"DEV={'✓' if self.vwap_deviation else '✗'}",
        ]
        mom = [
            f"ROC={'✓' if self.roc_confirm else '✗'}({self.roc_value:+.2f}%)",
            f"MACD={'✓' if self.macd_confirm else '✗'}({self.macd_histogram:+.4f})",
            f"CC={'✓' if self.candle_confirm else '✗'}",
        ]
        mtf = f"MTF={'✓' if self.mtf_ok else '✗'}"
        return (
            f"[{self.symbol}] {self.direction or 'NONE'} "
            f"base={self.score}/5 mom={self.momentum_score}/3 | "
            f"{' '.join(base)} | {' '.join(mom)} | {mtf} | "
            f"price={self.price:.2f} SL={self.stop_loss:.2f} TP={self.take_profit:.2f}"
        )


# ── Main strategy class ───────────────────────────────────────────────────────

class EMAVWAPStrategy:

    def __init__(self):
        log.info("[STRAT] EMA/VWAP strategy loaded ✓ (v3 — two-layer gate)")

    def _get(self, key, default):
        return get_config_override(key, default)

    def _val(self, sym_profile, profile_key, override_key, default):
        if sym_profile and sym_profile.get(profile_key) is not None:
            return float(sym_profile[profile_key])
        return float(self._get(override_key, default))

    def _is_prime_time(self) -> bool:
        """9:30am–PRIME_END_HOUR ET is prime time — lower score threshold."""
        now  = datetime.now(ET)
        end  = int(self._get("PRIME_END_HOUR", getattr(config, "PRIME_END_HOUR", 11)))
        return (now.hour == 9 and now.minute >= 30) or (9 < now.hour < end)

    def seed_volume_history(self, symbol: str, candle_volumes: list):
        _vol_tracker.seed_from_candle_volumes(symbol, candle_volumes)

    def on_tick(self, symbol: str, volume: int, candle_minute):
        _vol_tracker.on_tick(symbol, volume, candle_minute)

    def evaluate(self, symbol: str, cache: dict,
                 candles: list = None,
                 elapsed_seconds: float = 30.0) -> Optional[Signal]:

        if cache is None or cache.get("stale"):
            return None

        price  = cache.get("price")
        ema9   = cache.get("ema9")
        ema21  = cache.get("ema21")
        atr    = cache.get("atr")
        rsi    = cache.get("rsi")
        vwap   = cache.get("vwap")

        if any(v is None for v in [price, ema9, ema21, atr, rsi, vwap]):
            return None
        if atr == 0:
            return None

        sym_profile = _profiler.get_profile(symbol)

        # ── Load config ───────────────────────────────────────
        rsi_ob   = float(self._get("RSI_OVERBOUGHT", config.RSI_OVERBOUGHT))
        rsi_os   = float(self._get("RSI_OVERSOLD",   config.RSI_OVERSOLD))
        stop_mult = self._val(sym_profile, "atr_stop_mult", "ATR_STOP_MULT",  config.ATR_STOP_MULT)
        tp_mult   = self._val(sym_profile, "atr_tp_mult",   "ATR_TP_MULT",    config.ATR_TP_MULT)

        # Base score threshold — lower in prime time
        if self._is_prime_time():
            base_min = int(self._get("PRIME_BASE_MIN",   getattr(config, "PRIME_BASE_MIN",   3)))
        else:
            base_min = int(self._get("REGULAR_BASE_MIN", getattr(config, "REGULAR_BASE_MIN", 4)))

        # Momentum gate
        mom_gate_enabled = int(self._get("MOMENTUM_GATE_ENABLED", getattr(config, "MOMENTUM_GATE_ENABLED", 1)))
        mom_gate_min     = int(self._get("MOMENTUM_GATE_MIN",     getattr(config, "MOMENTUM_GATE_MIN",     2)))

        # MTF
        mtf_enabled  = int(self._get("MTF_FILTER_ENABLED", getattr(config, "MTF_FILTER_ENABLED", 1)))
        mtf_period   = int(self._get("MTF_EMA_PERIOD",     getattr(config, "MTF_EMA_PERIOD",     21)))

        # ROC
        roc_period    = int(self._get("ROC_PERIOD",    getattr(config, "ROC_PERIOD",    3)))
        roc_min_long  = float(self._get("ROC_MIN_LONG", getattr(config, "ROC_MIN_LONG", 0.08)))
        roc_min_short = float(self._get("ROC_MIN_SHORT", getattr(config, "ROC_MIN_SHORT", -0.08)))

        # MACD
        macd_fast   = int(self._get("MACD_FAST",          getattr(config, "MACD_FAST",          12)))
        macd_slow   = int(self._get("MACD_SLOW",          getattr(config, "MACD_SLOW",          26)))
        macd_signal = int(self._get("MACD_SIGNAL_PERIOD", getattr(config, "MACD_SIGNAL_PERIOD", 9)))

        # Candle consistency
        cc_lookback = int(self._get("CANDLE_CONSISTENCY_LOOKBACK", getattr(config, "CANDLE_CONSISTENCY_LOOKBACK", 3)))
        cc_min      = int(self._get("CANDLE_CONSISTENCY_MIN",      getattr(config, "CANDLE_CONSISTENCY_MIN",      2)))

        # VWAP deviation
        vwap_dev_mult  = float(self._get("VWAP_DEV_MULT",  getattr(config, "VWAP_DEV_MULT",  1.5)))
        vol_accel_mult = float(self._get("VOL_ACCEL_MULT", getattr(config, "VOL_ACCEL_MULT", 1.8)))

        # ATR floor
        min_atr_pct   = float(sym_profile["min_atr_pct"])   if sym_profile and sym_profile.get("min_atr_pct")   else 0.003
        min_atr_floor = float(sym_profile["min_atr_floor"]) if sym_profile and sym_profile.get("min_atr_floor") else price * min_atr_pct
        atr = max(atr, min_atr_floor)

        # ── Direction ─────────────────────────────────────────
        if ema9 > ema21:
            direction = "long"
        elif ema9 < ema21:
            direction = "short"
        else:
            return None

        candles = candles or []

        # ══════════════════════════════════════════════════════
        # BASE SIGNALS (5) — validate the setup
        # ══════════════════════════════════════════════════════

        # 1. EMA trend
        ema_ok = (ema9 > ema21) if direction == "long" else (ema9 < ema21)

        # 2. VWAP side
        vwap_ok = (price > vwap) if direction == "long" else (price < vwap)

        # 3. Volume acceleration
        vol_ratio = _vol_tracker.get_volume_ratio(symbol, elapsed_seconds)
        vol_ok    = vol_ratio >= vol_accel_mult

        # 4. RSI confirmation
        rsi_ok = (rsi < rsi_ob) if direction == "long" else (rsi > rsi_os)

        # 5. VWAP deviation
        dev_ok = _check_vwap_deviation(candles, price, vwap, direction, vwap_dev_mult)

        base_score = sum([ema_ok, vwap_ok, vol_ok, rsi_ok, dev_ok])

        # ══════════════════════════════════════════════════════
        # MOMENTUM SIGNALS (3) — validate the timing
        # ══════════════════════════════════════════════════════

        # 6. ROC — price acceleration (speed)
        roc_value = _calc_roc(candles, period=roc_period)
        roc_ok    = (roc_value >= roc_min_long) if direction == "long" else (roc_value <= roc_min_short)

        # 7. MACD — momentum building (build)
        macd_long_ok, macd_short_ok, macd_hist = _calc_macd(candles, macd_fast, macd_slow, macd_signal)
        macd_ok = macd_long_ok if direction == "long" else macd_short_ok

        # 8. Candle consistency — sustained pressure (persistence)
        candle_ok = _calc_candle_consistency(candles, cc_lookback, cc_min, direction)

        momentum_score = sum([roc_ok, macd_ok, candle_ok])

        # ══════════════════════════════════════════════════════
        # MULTI-TIMEFRAME FILTER
        # ══════════════════════════════════════════════════════
        mtf_ok = True
        if mtf_enabled:
            mtf_ok = _check_mtf_filter(candles, direction, mtf_period)

        # ══════════════════════════════════════════════════════
        # GATE CHECK
        # ══════════════════════════════════════════════════════
        base_pass  = base_score >= base_min
        mom_pass   = (not mom_gate_enabled) or (momentum_score >= mom_gate_min)
        mtf_pass   = mtf_ok

        direction_active = direction if (base_pass and mom_pass and mtf_pass) else None

        # ── Stops and TP ──────────────────────────────────────
        if direction == "long":
            stop_loss   = price - (atr * stop_mult)
            take_profit = price + (atr * tp_mult)
        else:
            stop_loss   = price + (atr * stop_mult)
            take_profit = price - (atr * tp_mult)

        # ── Log to DB (using base signals for existing columns) ─
        sig_id = log_signal(
            symbol=symbol,
            score=base_score,
            direction=direction_active,
            ema_cross=ema_ok,
            vwap_side=vwap_ok,
            volume_spike=vol_ok,
            rsi_confirm=rsi_ok,
            price_action=dev_ok,
            price=price,
            atr=atr
        )

        signal = Signal(
            symbol=symbol,
            direction=direction_active,
            score=base_score,
            momentum_score=momentum_score,
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
            macd_confirm=macd_ok,
            candle_confirm=candle_ok,
            mtf_ok=mtf_ok,
            roc_value=roc_value,
            macd_histogram=macd_hist,
            signal_id=sig_id
        )

        log.info(f"[STRAT] {signal}")
        return signal
