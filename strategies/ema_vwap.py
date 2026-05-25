"""
strategies/ema_vwap.py — EMA + VWAP Momentum Strategy

Evaluates 5 signals and scores them 0-5.
A trade fires when score >= MIN_SIGNAL_SCORE.

Reads from the live stream cache (PriceStream.get_price) and
completed candle history (PriceStream.get_candles) for price action.

Signals:
  1. EMA trend      — EMA9 above/below EMA21
  2. VWAP side      — price above/below VWAP
  3. Volume spike   — current tick volume > N x recent average
  4. RSI confirm    — RSI not overbought (long) / oversold (short)
  5. Price action   — last 3 completed candles making HH+HL (long)
                      or LH+LL (short). Uses real candle high/low
                      from the candle builder so PA actually fires.
"""

import logging
from dataclasses import dataclass
from typing import Optional, List
from core.database import get_config_override, log_signal
from meta.symbol_profiler import SymbolProfiler

_profiler = SymbolProfiler()
import config

log = logging.getLogger(__name__)


@dataclass
class Signal:
    symbol:       str
    direction:    Optional[str]
    score:        int
    price:        float
    atr:          float
    stop_loss:    float
    take_profit:  float
    ema_cross:    bool
    vwap_side:    bool
    vol_spike:    bool
    rsi_confirm:  bool
    price_action: bool
    signal_id:    Optional[str] = None

    def __str__(self):
        checks = [
            f"EMA={'✓' if self.ema_cross else '✗'}",
            f"VWAP={'✓' if self.vwap_side else '✗'}",
            f"VOL={'✓' if self.vol_spike else '✗'}",
            f"RSI={'✓' if self.rsi_confirm else '✗'}",
            f"PA={'✓' if self.price_action else '✗'}",
        ]
        return (
            f"[{self.symbol}] {self.direction or 'NONE'} "
            f"score={self.score}/5 | {' '.join(checks)} "
            f"| price={self.price:.2f} SL={self.stop_loss:.2f} "
            f"TP={self.take_profit:.2f}"
        )


def _check_price_action(candles: list, direction: str) -> bool:
    """
    Check if recent candles show the right price action structure.

    Requires at least 3 completed candles. Looks at the last 3 candles
    and checks whether they're making:
      - Higher highs AND higher lows (bullish, for longs)
      - Lower highs AND lower lows  (bearish, for shorts)

    Each candle is a dict with keys: high, low, close

    We check consecutive pairs — candle[1] vs candle[0], candle[2] vs candle[1].
    Both pairs must agree for PA to fire. This filters out single-candle fakeouts.

    Example (long):
      candle[0]: high=100, low=98
      candle[1]: high=101, low=99   <- HH (101>100) and HL (99>98) ✓
      candle[2]: high=102, low=100  <- HH (102>101) and HL (100>99) ✓
      → PA = True

    Example (long, fails):
      candle[0]: high=100, low=98
      candle[1]: high=101, low=97   <- HH but LL ✗
      → PA = False
    """
    if not candles or len(candles) < 3:
        return False

    # take the 3 most recent completed candles
    c = candles[-3:]

    if direction == "long":
        # each successive candle must have a higher high AND higher low
        hh_hl_1 = (c[1]["high"] > c[0]["high"]) and (c[1]["low"] > c[0]["low"])
        hh_hl_2 = (c[2]["high"] > c[1]["high"]) and (c[2]["low"] > c[1]["low"])
        return hh_hl_1 and hh_hl_2

    else:  # short
        # each successive candle must have a lower high AND lower low
        lh_ll_1 = (c[1]["high"] < c[0]["high"]) and (c[1]["low"] < c[0]["low"])
        lh_ll_2 = (c[2]["high"] < c[1]["high"]) and (c[2]["low"] < c[1]["low"])
        return lh_ll_1 and lh_ll_2


class EMAVWAPStrategy:
    """
    EMA + VWAP momentum strategy.
    Reads config overrides from DB so meta brain can tune thresholds live.
    """

    def __init__(self):
        log.info("[STRAT] EMA/VWAP strategy loaded ✓")
        self._vol_window = {}   # symbol -> list of recent tick volumes

    def _get_threshold(self, key, default):
        return get_config_override(key, default)

    def _update_vol_window(self, symbol: str, volume: int, maxlen: int = 20):
        if symbol not in self._vol_window:
            self._vol_window[symbol] = []
        self._vol_window[symbol].append(volume)
        if len(self._vol_window[symbol]) > maxlen:
            self._vol_window[symbol].pop(0)

    def _vol_spike_mult(self, symbol: str, current_vol: int) -> float:
        """Current volume relative to recent average. 1.0 if not enough data."""
        window = self._vol_window.get(symbol, [])
        if len(window) < 5:
            return 1.0
        # compare current against average of all but current
        avg = sum(window[:-1]) / len(window[:-1])
        if avg == 0:
            return 1.0
        return current_vol / avg

    def evaluate(self, symbol: str, cache: dict, candles: list = None) -> Optional[Signal]:
        """
        Evaluate current stream data and return a Signal.

        Args:
            symbol:  ticker symbol
            cache:   dict from PriceStream.get_price(symbol)
            candles: list of completed 1-min candle dicts from
                     PriceStream.get_candles(symbol). Each candle
                     has keys: high, low, close. Used for PA signal.
                     Defaults to None (PA will be False).

        Returns Signal or None if data is insufficient.
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

        # all core values must be present
        if any(v is None for v in [price, ema9, ema21, atr, rsi, vwap]):
            log.debug(f"[STRAT] {symbol}: incomplete indicators")
            return None

        if atr == 0:
            log.debug(f"[STRAT] {symbol}: ATR is 0 — skipping")
            return None

        # ── Tunable thresholds — symbol profile > DB override > config default ──
        # 1. Load symbol-specific profile if it exists
        sym_profile  = _profiler.get_profile(symbol)

        # 2. Helper: profile value → DB override → config default
        def _val(profile_key, override_key, default):
            if sym_profile and sym_profile.get(profile_key) is not None:
                return float(sym_profile[profile_key])
            return float(self._get_threshold(override_key, default))

        min_score    = int(self._get_threshold("MIN_SIGNAL_SCORE", config.MIN_SIGNAL_SCORE))
        vol_mult_req = _val("volume_spike_mult", "VOLUME_SPIKE_MULT", config.VOLUME_SPIKE_MULT)
        rsi_ob       = float(self._get_threshold("RSI_OVERBOUGHT",   config.RSI_OVERBOUGHT))
        rsi_os       = float(self._get_threshold("RSI_OVERSOLD",     config.RSI_OVERSOLD))
        stop_mult    = _val("atr_stop_mult", "ATR_STOP_MULT", config.ATR_STOP_MULT)
        tp_mult      = _val("atr_tp_mult",   "ATR_TP_MULT",  config.ATR_TP_MULT)
        be_mult      = _val("breakeven_mult","BREAKEVEN_ATR_MULT", config.BREAKEVEN_ATR_MULT)

        # 3. Apply ATR floor — prevents TQQQ-style issues where ATR is
        #    so small relative to price that stops get placed above entry
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

        # ── Signal 1: EMA trend ───────────────────────────────
        # EMA9 above EMA21 = bullish trend, below = bearish
        ema_ok = (ema9 > ema21) if direction == "long" else (ema9 < ema21)

        # ── Signal 2: VWAP side ───────────────────────────────
        if direction == "long":
            vwap_ok = price > vwap
        else:
            vwap_ok = price < vwap

        # ── Signal 3: Volume spike ────────────────────────────
        self._update_vol_window(symbol, volume)
        vol_ratio = self._vol_spike_mult(symbol, volume)
        vol_ok    = vol_ratio >= vol_mult_req

        # ── Signal 4: RSI confirmation ────────────────────────
        if direction == "long":
            rsi_ok = rsi < rsi_ob   # not overbought
        else:
            rsi_ok = rsi > rsi_os   # not oversold

        # ── Signal 5: Price action ────────────────────────────
        # Uses real 1-minute candle high/low from candle builder.
        # Checks that the last 3 completed candles are making
        # higher highs + higher lows (long) or lower highs + lower lows (short).
        # Falls back to False if fewer than 3 candles have completed.
        pa_ok = _check_price_action(candles or [], direction)

        # ── Score ─────────────────────────────────────────────
        score = sum([ema_ok, vwap_ok, vol_ok, rsi_ok, pa_ok])

        # ── Calculate stops and take profit ───────────────────
        if direction == "long":
            stop_loss   = price - (atr * stop_mult)
            take_profit = price + (atr * tp_mult)
        else:
            stop_loss   = price + (atr * stop_mult)
            take_profit = price - (atr * tp_mult)

        # ── Log to DB for meta brain ──────────────────────────
        sig_id = log_signal(
            symbol=symbol,
            score=score,
            direction=direction if score >= min_score else None,
            ema_cross=ema_ok,
            vwap_side=vwap_ok,
            volume_spike=vol_ok,
            rsi_confirm=rsi_ok,
            price_action=pa_ok,
            price=price,
            atr=atr
        )

        signal = Signal(
            symbol=symbol,
            direction=direction if score >= min_score else None,
            score=score,
            price=price,
            atr=atr,
            stop_loss=round(stop_loss, 4),
            take_profit=round(take_profit, 4),
            ema_cross=ema_ok,
            vwap_side=vwap_ok,
            vol_spike=vol_ok,
            rsi_confirm=rsi_ok,
            price_action=pa_ok,
            signal_id=sig_id
        )

        log.info(f"[STRAT] {signal}")
        return signal
