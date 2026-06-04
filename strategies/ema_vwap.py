"""
strategies/ema_vwap.py — EMA + VWAP Momentum Strategy v5

v5 changes (5-min bars + new signal stack + richer Claude context):

TIMEFRAME: Now operates on 5-minute candles from the stream.
  1-min was too noisy. 5-min has real structure that indicators can read.

SIGNAL STACK (rebuilt from research):

  Gate 0 — ADX regime filter (new, critical)
    ADX < 20 = market is choppy/ranging → block ALL entries
    ADX ≥ 20 = market is trending → allow evaluation
    This alone would have blocked most bad trades in sideways sessions.

  Gate 1 — Claude Haiku direction
    Sees: 5-min candles, price, VWAP + slope, RSI (50-line not 70/30),
          ADX, opening range high/low, session relative volume, EMA state
    Returns: long / short / none
    Fallback: rule-based direction using the same data (no API needed)

  Base signals — need 3 of 4 (was 5 of 5, simplified):
    1. VWAP side      — price above for long, below for short
    2. VWAP deviation — price meaningfully extended from VWAP
    3. EMA agreement  — EMA9/21 agrees with Claude direction
    4. Volume spike   — current candle volume 1.5x+ average

  Momentum gate — need 1 of 2 (was 2 of 3, simplified + faster MACD):
    1. MACD histogram growing in direction (faster 5-13-9 settings)
    2. RSI above 50 for longs / below 50 for shorts (50-line not 70/30)

  MTF filter (unchanged): existing multi-timeframe EMA filter

DROPPED: ROC (redundant with MACD), Candle Consistency (redundant with EMA+MACD)

FALLBACK DIRECTION (new):
  When Claude API is unavailable, uses a scored rule-based system:
    +1 each: VWAP side, EMA direction, RSI 50-line, MACD direction, ADX>25
    Score ≥ 3 = long or short, score < 3 = none
  This is genuinely independent from Claude and works without internet.

API HEALTH TRACKING:
  Tracks consecutive successes/failures. Logs warnings when fallback
  is being used repeatedly so you know if the API is degraded.
"""

import math
import logging
import requests as _requests
import pytz
from dataclasses import dataclass
from typing import Optional
from collections import deque
from datetime import datetime
from core.database import get_config_override, log_signal
from meta.symbol_profiler import SymbolProfiler

_profiler = SymbolProfiler()
import config

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")

# ── API health tracker ────────────────────────────────────────────────────────

class _APIHealthTracker:
    """Tracks Claude API call success/failure rate."""
    def __init__(self):
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.total_calls     = 0
        self.total_failures  = 0

    def record_success(self, symbol: str):
        self.consecutive_failures  = 0
        self.consecutive_successes += 1
        self.total_calls += 1
        if self.consecutive_successes == 1:
            log.info(f"[CLAUDE-DIR] ✓ API recovered for {symbol}")
        # Write health to DB every 10 calls so dashboard can read it
        if self.total_calls % 10 == 0:
            self._persist()

    def record_failure(self, symbol: str, reason: str):
        self.consecutive_successes = 0
        self.consecutive_failures += 1
        self.total_calls    += 1
        self.total_failures += 1
        if self.consecutive_failures in (1, 3, 10):
            pct = round(self.total_failures / self.total_calls * 100)
            log.warning(
                f"[CLAUDE-DIR] ⚠ API failure #{self.consecutive_failures} "
                f"for {symbol}: {reason} | "
                f"failure rate={pct}% ({self.total_failures}/{self.total_calls})"
            )
        self._persist()

    def _persist(self):
        """Write health stats to DB so dashboard /api/health can read them."""
        try:
            from core.database import set_config_override
            set_config_override("CLAUDE_API_FAILURES", self.consecutive_failures)
            set_config_override("CLAUDE_API_TOTAL",    self.total_calls)
            if self.consecutive_failures == 0:
                set_config_override("CLAUDE_LAST_SUCCESS", "ok")
        except Exception:
            pass  # never crash the bot over health tracking

    def is_degraded(self) -> bool:
        return self.consecutive_failures >= 3

_api_health = _APIHealthTracker()


# ── Rule-based fallback direction ─────────────────────────────────────────────

def _fallback_direction(cache: dict, candles: list) -> str:
    """
    Scored rule-based direction when Claude API is unavailable.

    Uses 5 genuinely independent signals, needs 3+ to commit to a direction.
    This is NOT the EMA cross shortcut — it's a real decision system.

    Returns 'long', 'short', or 'none'.
    """
    price  = cache.get("price", 0)
    ema9   = cache.get("ema9",  0)
    ema21  = cache.get("ema21", 0)
    rsi    = cache.get("rsi",   50)
    vwap   = cache.get("vwap",  0)
    adx    = cache.get("adx",   0)

    if not price or not vwap:
        return "none"

    # ADX gate — if market is ranging, return none regardless
    if adx < 15:
        log.debug(f"[FALLBACK-DIR] ADX={adx:.1f} < 15 — ranging market, returning none")
        return "none"

    # MACD from candles
    macd_long = macd_short = False
    if len(candles) >= 30:
        closes = [c["close"] for c in candles]
        ml, ms, _ = _calc_macd(candles, fast=5, slow=13, signal_period=9)
        macd_long  = ml
        macd_short = ms

    # Score each direction independently
    long_score  = 0
    short_score = 0

    # 1. VWAP side
    if price > vwap: long_score  += 1
    else:            short_score += 1

    # 2. EMA direction
    if ema9 > ema21: long_score  += 1
    elif ema9 < ema21: short_score += 1

    # 3. RSI 50-line (momentum direction, not overbought/oversold)
    if rsi > 52:   long_score  += 1
    elif rsi < 48: short_score += 1

    # 4. MACD histogram
    if macd_long:  long_score  += 1
    if macd_short: short_score += 1

    # 5. ADX confirms strong trend (bonus point for conviction)
    if adx > 25:
        if ema9 > ema21: long_score  += 1
        else:            short_score += 1

    threshold = 3
    if long_score >= threshold and long_score > short_score:
        log.info(f"[FALLBACK-DIR] → long  (score={long_score}/5, ADX={adx:.1f})")
        return "long"
    if short_score >= threshold and short_score > long_score:
        log.info(f"[FALLBACK-DIR] → short (score={short_score}/5, ADX={adx:.1f})")
        return "short"

    log.debug(f"[FALLBACK-DIR] → none (long={long_score} short={short_score} ADX={adx:.1f})")
    return "none"


# ── Claude direction call ─────────────────────────────────────────────────────

def _claude_direction(symbol: str, cache: dict, candles: list,
                      atr: float) -> str:
    """
    Ask Claude Haiku for trade direction using rich 5-min context.
    Returns 'long', 'short', or 'none'.

    Falls back to _fallback_direction() on any API error — the fallback
    is a real scored system, not just EMA cross.

    API health is tracked globally and logged when degraded.
    """
    api_key = getattr(config, "ANTHROPIC_API_KEY", "")

    if not api_key:
        log.debug(f"[CLAUDE-DIR] No API key — using rule-based fallback for {symbol}")
        return _fallback_direction(cache, candles)

    # Pull all context from cache
    price      = cache.get("price", 0)
    ema9       = cache.get("ema9",  0)
    ema21      = cache.get("ema21", 0)
    rsi        = cache.get("rsi",   50)
    vwap       = cache.get("vwap",  0)
    adx        = cache.get("adx",   0)
    vwap_slope = cache.get("vwap_slope", "flat")
    or_high    = cache.get("opening_range_high")
    or_low     = cache.get("opening_range_low")
    rel_vol    = cache.get("relative_volume", 1.0)

    # Build 5-min candle summary (last 12 = 1 hour of data)
    recent = candles[-12:] if len(candles) >= 12 else candles
    candle_lines = []
    for i, c in enumerate(recent):
        body_dir = "▲" if c["close"] >= c.get("open", c["close"]) else "▼"
        candle_lines.append(
            f"  [{i+1:2d}] {body_dir} H={c['high']:.2f} "
            f"L={c['low']:.2f} C={c['close']:.2f} V={c.get('volume', 0):,}"
        )
    candle_str = "\n".join(candle_lines) if candle_lines else "  (insufficient data)"

    # Opening range context
    if or_high and or_low:
        or_width = or_high - or_low
        if price > or_high:
            or_context = f"ABOVE opening range (range: ${or_low:.2f}–${or_high:.2f}, width=${or_width:.2f})"
        elif price < or_low:
            or_context = f"BELOW opening range (range: ${or_low:.2f}–${or_high:.2f}, width=${or_width:.2f})"
        else:
            or_context = f"INSIDE opening range (range: ${or_low:.2f}–${or_high:.2f}, width=${or_width:.2f})"
    else:
        or_context = "Opening range not yet established (pre-9:45 AM)"

    vwap_dist_pct = abs(price - vwap) / vwap * 100 if vwap > 0 else 0
    ema_state = "EMA9 > EMA21 (bullish)" if ema9 > ema21 else "EMA9 < EMA21 (bearish)"

    # ADX interpretation
    if adx < 15:
        adx_desc = "very weak/ranging — poor conditions for momentum trades"
    elif adx < 20:
        adx_desc = "weak trend — marginal conditions"
    elif adx < 30:
        adx_desc = "moderate trend — acceptable for entries"
    elif adx < 40:
        adx_desc = "strong trend — good conditions"
    else:
        adx_desc = "very strong trend — excellent conditions but watch for exhaustion"

    prompt = f"""You are a direction filter for AlphaBot, a momentum day trading bot trading {symbol}.

Your ONLY job: decide if price is more likely to go UP (long), DOWN (short), or is too uncertain (none) over the next 15-30 minutes.

=== MARKET REGIME ===
ADX: {adx:.1f} — {adx_desc}
Session relative volume: {rel_vol:.2f}x normal {"(low participation — be cautious)" if rel_vol < 0.7 else "(normal)" if rel_vol < 1.3 else "(high participation — good conditions)"}

=== PRICE CONTEXT ===
Current price: ${price:.2f}
VWAP: ${vwap:.2f} — price is {"ABOVE" if price > vwap else "BELOW"} by {vwap_dist_pct:.2f}%
VWAP slope: {vwap_slope.upper()} {"(institutions consistently buying)" if vwap_slope == "up" else "(institutions consistently selling)" if vwap_slope == "down" else "(no clear institutional direction)"}
EMA: {ema_state}
RSI: {rsi:.1f} {"(bullish momentum)" if rsi > 55 else "(bearish momentum)" if rsi < 45 else "(neutral/mixed)"}
ATR: {atr:.4f} (normal volatility per 5-min bar)

=== OPENING RANGE ===
Price is {or_context}

=== LAST {len(recent)} FIVE-MINUTE CANDLES (oldest → newest) ===
{candle_str}

=== DECISION FRAMEWORK ===
STRONG LONG signals: price above VWAP + VWAP sloping up + price above opening range + RSI > 55 + ADX > 25
STRONG SHORT signals: price below VWAP + VWAP sloping down + price below opening range + RSI < 45 + ADX > 25
SAY NONE if: ADX < 20 (ranging), mixed signals, low relative volume, or low conviction

RSI rules for momentum: above 50 = bullish momentum, below 50 = bearish (NOT overbought/oversold)
Only say none if you genuinely cannot determine direction with confidence.

Respond with EXACTLY one word: long, short, or none"""

    try:
        r = _requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         api_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json"
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 5,
                "messages":   [{"role": "user", "content": prompt}]
            },
            timeout=8
        )
        r.raise_for_status()
        answer = r.json()["content"][0]["text"].strip().lower().rstrip(".")

        if answer in ("long", "short", "none"):
            _api_health.record_success(symbol)
            log.info(
                f"[CLAUDE-DIR] {symbol} → {answer} "
                f"(ADX={adx:.1f} RSI={rsi:.1f} "
                f"VWAP={vwap_slope} rel_vol={rel_vol:.1f}x)"
            )
            return answer

        log.warning(f"[CLAUDE-DIR] {symbol} unexpected response: '{answer}' — using fallback")
        _api_health.record_failure(symbol, f"unexpected response: {answer}")

    except _requests.exceptions.Timeout:
        _api_health.record_failure(symbol, "timeout (8s)")
    except _requests.exceptions.ConnectionError:
        _api_health.record_failure(symbol, "connection error")
    except Exception as e:
        _api_health.record_failure(symbol, str(e)[:60])

    # Fallback — rule-based, genuinely independent
    log.info(f"[CLAUDE-DIR] {symbol} using rule-based fallback (API failures={_api_health.consecutive_failures})")
    return _fallback_direction(cache, candles)


# ── EMA helpers ───────────────────────────────────────────────────────────────

def _ema_list(prices: list, period: int) -> list:
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    result = [ema]
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
        result.append(ema)
    return result


# ── MACD (faster settings for 5-min bars) ────────────────────────────────────

def _calc_macd(candles: list, fast: int = 5, slow: int = 13,
               signal_period: int = 9) -> tuple:
    """
    MACD histogram with faster 5-13-9 settings for 5-min charts.
    Standard 12-26-9 lags too much on intraday.

    Returns (long_ok, short_ok, histogram_value)
    long_ok:  histogram positive AND growing
    short_ok: histogram negative AND falling
    """
    min_candles = slow + signal_period + 2
    if len(candles) < min_candles:
        return False, False, 0.0

    closes   = [c["close"] for c in candles]
    ema_fast = _ema_list(closes, fast)
    ema_slow = _ema_list(closes, slow)

    if not ema_fast or not ema_slow:
        return False, False, 0.0

    offset = slow - fast
    if offset < 0 or offset >= len(ema_fast):
        return False, False, 0.0

    macd_line = [ema_fast[i + offset] - ema_slow[i] for i in range(len(ema_slow))]

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


# ── VWAP standard deviation ───────────────────────────────────────────────────

def _calc_vwap_std(candles: list, vwap: float, lookback: int = 10) -> float:
    if not candles or len(candles) < 3:
        return 0.0
    recent   = [c["close"] for c in candles[-lookback:]]
    mean     = sum(recent) / len(recent)
    variance = sum((c - mean) ** 2 for c in recent) / len(recent)
    return math.sqrt(variance)


def _check_vwap_deviation(candles: list, price: float, vwap: float,
                           direction: str, std_mult: float = 1.0) -> bool:
    """
    Check if price is meaningfully extended from VWAP in signal direction.
    Uses 1.0 std dev threshold (was 1.5) since 5-min bars have larger moves.
    """
    std = _calc_vwap_std(candles, vwap)
    if std == 0.0:
        return (price > vwap) if direction == "long" else (price < vwap)
    deviation = abs(price - vwap)
    if direction == "long":
        return price > vwap and deviation >= std_mult * std
    else:
        return price < vwap and deviation >= std_mult * std


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
        # For 5-min bars, project over 300 seconds
        projected = cur_vol * (300.0 / elapsed_seconds)
        return projected / avg


_vol_tracker = VolumeAccelerationTracker(window=20)


# ── Multi-timeframe filter ────────────────────────────────────────────────────

def _check_mtf_filter(candles: list, direction: str,
                       ema_period: int = 21) -> bool:
    """MTF filter on 5-min candles. EMA21 on 5-min ≈ ~100-min trend."""
    if len(candles) < ema_period + 2:
        return True
    closes   = [c["close"] for c in candles]
    ema_vals = _ema_list(closes, ema_period)
    if len(ema_vals) < 2:
        return True
    trend_up = ema_vals[-1] > ema_vals[-2]
    return trend_up if direction == "long" else not trend_up


# ── Signal dataclass ──────────────────────────────────────────────────────────

@dataclass
class Signal:
    symbol:         str
    direction:      Optional[str]
    score:          int
    momentum_score: int
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
            f"DEV={'✓' if self.vwap_deviation else '✗'}",
        ]
        mom = [
            f"MACD={'✓' if self.macd_confirm else '✗'}({self.macd_histogram:+.4f})",
            f"RSI={'✓' if self.rsi_confirm else '✗'}",
        ]
        mtf = f"MTF={'✓' if self.mtf_ok else '✗'}"
        return (
            f"[{self.symbol}] {self.direction or 'NONE'} "
            f"base={self.score}/4 mom={self.momentum_score}/2 | "
            f"{' '.join(base)} | {' '.join(mom)} | {mtf} | "
            f"price={self.price:.2f} SL={self.stop_loss:.2f} TP={self.take_profit:.2f}"
        )


# ── Main strategy class ───────────────────────────────────────────────────────

class EMAVWAPStrategy:

    def __init__(self):
        log.info("[STRAT] EMA/VWAP strategy loaded ✓ (v5 — 5-min bars, ADX gate, rule-based fallback)")

    def _get(self, key, default):
        return get_config_override(key, default)

    def _val(self, sym_profile, profile_key, override_key, default):
        if sym_profile and sym_profile.get(profile_key) is not None:
            return float(sym_profile[profile_key])
        return float(self._get(override_key, default))

    def _is_prime_time(self) -> bool:
        now = datetime.now(ET)
        end = int(self._get("PRIME_END_HOUR", getattr(config, "PRIME_END_HOUR", 11)))
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
        adx    = cache.get("adx", 0)

        if any(v is None for v in [price, ema9, ema21, atr, rsi, vwap]):
            return None
        if atr == 0:
            return None

        sym_profile = _profiler.get_profile(symbol)
        candles     = candles or []

        # ── Load config ───────────────────────────────────────
        rsi_ob    = float(self._get("RSI_OVERBOUGHT", config.RSI_OVERBOUGHT))
        rsi_os    = float(self._get("RSI_OVERSOLD",   config.RSI_OVERSOLD))
        stop_mult = self._val(sym_profile, "atr_stop_mult", "ATR_STOP_MULT", config.ATR_STOP_MULT)
        tp_mult   = self._val(sym_profile, "atr_tp_mult",   "ATR_TP_MULT",   config.ATR_TP_MULT)

        # Base score threshold
        if self._is_prime_time():
            base_min = int(self._get("PRIME_BASE_MIN",   getattr(config, "PRIME_BASE_MIN",   3)))
        else:
            base_min = int(self._get("REGULAR_BASE_MIN", getattr(config, "REGULAR_BASE_MIN", 4)))
        # Clamp to new max of 4
        base_min = min(base_min, 4)

        # Momentum gate
        mom_gate_enabled = int(self._get("MOMENTUM_GATE_ENABLED", getattr(config, "MOMENTUM_GATE_ENABLED", 1)))

        # MTF
        mtf_enabled = int(self._get("MTF_FILTER_ENABLED", getattr(config, "MTF_FILTER_ENABLED", 1)))
        mtf_period  = int(self._get("MTF_EMA_PERIOD",     getattr(config, "MTF_EMA_PERIOD",     21)))

        # Volume
        vol_accel_mult = float(self._get("VOL_ACCEL_MULT", getattr(config, "VOL_ACCEL_MULT", 1.5)))

        # VWAP deviation
        vwap_dev_mult = float(self._get("VWAP_DEV_MULT", getattr(config, "VWAP_DEV_MULT", 1.0)))

        # ATR floor
        min_atr_pct   = float(sym_profile["min_atr_pct"])   if sym_profile and sym_profile.get("min_atr_pct")   else 0.003
        min_atr_floor = float(sym_profile["min_atr_floor"]) if sym_profile and sym_profile.get("min_atr_floor") else price * min_atr_pct
        atr = max(atr, min_atr_floor)

        # ══════════════════════════════════════════════════════
        # GATE 0 — ADX REGIME FILTER
        # Block all entries when market is ranging/choppy.
        # ADX < 20 = no trend = momentum signals are noise.
        # ══════════════════════════════════════════════════════
        adx_threshold = float(self._get("ADX_MIN_THRESHOLD", 20.0))
        if adx < adx_threshold:
            log.debug(
                f"[STRAT] {symbol} blocked by ADX gate — "
                f"ADX={adx:.1f} < {adx_threshold} (ranging market)"
            )
            # Log the skip for diagnostics
            log_signal(
                symbol=symbol, score=0, direction=None,
                ema_cross=False, vwap_side=False, volume_spike=False,
                rsi_confirm=False, price_action=False, price=price, atr=atr,
                roc_confirm=False, macd_confirm=False, candle_confirm=False,
                mtf_ok=False, momentum_score=0, roc_value=adx, macd_histogram=0.0
            )
            return None

        # ══════════════════════════════════════════════════════
        # GATE 1 — CLAUDE DIRECTION (with rule-based fallback)
        # ══════════════════════════════════════════════════════
        direction = _claude_direction(symbol, cache, candles, atr)
        if direction == "none":
            log_signal(
                symbol=symbol, score=0, direction=None,
                ema_cross=False, vwap_side=False, volume_spike=False,
                rsi_confirm=False, price_action=False, price=price, atr=atr,
                roc_confirm=False, macd_confirm=False, candle_confirm=False,
                mtf_ok=False, momentum_score=0, roc_value=adx, macd_histogram=0.0
            )
            return None

        # ══════════════════════════════════════════════════════
        # BASE SIGNALS (4) — need 3 to enter
        # Each is genuinely independent from the others
        # ══════════════════════════════════════════════════════

        # 1. EMA agreement with Claude direction
        ema_ok = (ema9 > ema21) if direction == "long" else (ema9 < ema21)

        # 2. VWAP side — institutional positioning
        vwap_ok = (price > vwap) if direction == "long" else (price < vwap)

        # 3. Volume spike — real participation
        vol_ratio = _vol_tracker.get_volume_ratio(symbol, elapsed_seconds)
        vol_ok    = vol_ratio >= vol_accel_mult

        # 4. VWAP deviation — meaningful extension in signal direction
        dev_ok = _check_vwap_deviation(candles, price, vwap, direction, vwap_dev_mult)

        base_score = sum([ema_ok, vwap_ok, vol_ok, dev_ok])

        # ══════════════════════════════════════════════════════
        # MOMENTUM GATE (2) — need 1 to enter
        # MACD and RSI-50 are different enough to keep both
        # ══════════════════════════════════════════════════════

        # 1. MACD histogram (faster 5-13-9 for 5-min bars)
        macd_long_ok, macd_short_ok, macd_hist = _calc_macd(
            candles, fast=5, slow=13, signal_period=9
        )
        macd_ok = macd_long_ok if direction == "long" else macd_short_ok

        # 2. RSI 50-line (momentum direction, NOT overbought/oversold)
        # Above 50 = bullish momentum, below 50 = bearish momentum
        rsi_ok = (rsi > 50) if direction == "long" else (rsi < 50)

        momentum_score = sum([macd_ok, rsi_ok])

        # ══════════════════════════════════════════════════════
        # MTF FILTER
        # ══════════════════════════════════════════════════════
        mtf_ok = True
        if mtf_enabled:
            mtf_ok = _check_mtf_filter(candles, direction, mtf_period)

        # ══════════════════════════════════════════════════════
        # GATE CHECK
        # ══════════════════════════════════════════════════════
        base_pass = base_score >= base_min      # need 3 of 4
        mom_pass  = (not mom_gate_enabled) or (momentum_score >= 1)  # need 1 of 2
        mtf_pass  = mtf_ok

        direction_active = direction if (base_pass and mom_pass and mtf_pass) else None

        # ── Stops and TP ──────────────────────────────────────
        if direction == "long":
            stop_loss   = price - (atr * stop_mult)
            take_profit = price + (atr * tp_mult)
        else:
            stop_loss   = price + (atr * stop_mult)
            take_profit = price - (atr * tp_mult)

        # ── Log to DB ─────────────────────────────────────────
        # Reuse existing columns: roc_value = ADX for dashboard visibility
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
            atr=atr,
            roc_confirm=False,        # ROC dropped
            macd_confirm=macd_ok,
            candle_confirm=False,     # Candle consistency dropped
            mtf_ok=mtf_ok,
            momentum_score=momentum_score,
            roc_value=adx,            # Store ADX here for dashboard visibility
            macd_histogram=macd_hist
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
            roc_confirm=False,
            macd_confirm=macd_ok,
            candle_confirm=False,
            mtf_ok=mtf_ok,
            roc_value=adx,
            macd_histogram=macd_hist,
            signal_id=sig_id
        )

        log.info(f"[STRAT] {signal}")
        return signal
