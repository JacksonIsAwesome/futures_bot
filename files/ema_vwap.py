"""
strategies/ema_vwap.py — EMA + VWAP Momentum Strategy

The core trading logic. Evaluates 5 signals and scores them.
A trade fires when MIN_SIGNAL_SCORE or more align.

Signals evaluated:
  1. EMA crossover    — 9 EMA crossed above/below 21 EMA
  2. VWAP side        — price is on the right side of VWAP
  3. Volume spike     — current volume > 1.5x 20-bar average
  4. RSI confirmation — RSI not overbought on longs / oversold on shorts
  5. Price action     — higher highs + higher lows (long) or opposite (short)

Entry: all signals align, score >= MIN_SIGNAL_SCORE
Stop:  ATR * ATR_STOP_MULT below entry
TP:    ATR * ATR_TP_MULT above entry (2:1 minimum R:R)
"""

import logging
from dataclasses import dataclass
from typing import Optional
from core.database import get_config_override, log_signal
import config

log = logging.getLogger(__name__)


@dataclass
class Signal:
    symbol:       str
    direction:    Optional[str]   # 'long', 'short', or None
    score:        int             # 0-5
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
        return (f"[{self.symbol}] {self.direction or 'NONE'} "
                f"score={self.score}/5 | {' '.join(checks)} "
                f"| price={self.price:.2f} SL={self.stop_loss:.2f} "
                f"TP={self.take_profit:.2f}")


class EMAVWAPStrategy:
    """
    EMA + VWAP momentum strategy.
    Reads live config overrides from DB so meta brain can tune it.
    """

    def __init__(self):
        log.info("[STRAT] EMA/VWAP strategy loaded ✓")

    def _get_threshold(self, key, default):
        """Pull threshold from DB overrides (meta brain adjustable)."""
        return get_config_override(key, default)

    def evaluate(self, symbol, df) -> Optional[Signal]:
        """
        Evaluate the latest bar and return a Signal object.
        Returns None if data is insufficient.
        """
        if df is None or len(df) < 50:
            log.debug(f"[STRAT] {symbol}: insufficient data")
            return None

        # pull current values from latest bar
        row      = df.iloc[-1]
        prev     = df.iloc[-2]
        price    = float(row["close"])
        atr      = float(row["atr"])
        vwap     = float(row["vwap"])
        rsi      = float(row["rsi"])
        vol_mult = float(row["vol_spike"])
        ema_fast = float(row["ema_fast"])
        ema_slow = float(row["ema_slow"])

        # get tunable thresholds (meta brain can adjust)
        min_score    = int(self._get_threshold("MIN_SIGNAL_SCORE", config.MIN_SIGNAL_SCORE))
        vol_mult_req = float(self._get_threshold("VOLUME_SPIKE_MULT", config.VOLUME_SPIKE_MULT))
        rsi_ob       = float(self._get_threshold("RSI_OVERBOUGHT",   config.RSI_OVERBOUGHT))
        rsi_os       = float(self._get_threshold("RSI_OVERSOLD",     config.RSI_OVERSOLD))

        # ── Determine direction from EMA crossover ────────────
        # Check if a crossover just happened in last 3 bars
        recent = df.iloc[-3:]
        long_cross  = any(recent["ema_cross"] > 0)   # fast crossed above slow
        short_cross = any(recent["ema_cross"] < 0)   # fast crossed below slow

        # also consider sustained trend if no fresh cross
        trend_long  = ema_fast > ema_slow
        trend_short = ema_fast < ema_slow

        # prefer fresh cross, fall back to trend
        if long_cross:
            direction = "long"
        elif short_cross:
            direction = "short"
        elif trend_long:
            direction = "long"
        elif trend_short:
            direction = "short"
        else:
            direction = None

        if direction is None:
            return None

        # ── Evaluate each signal ──────────────────────────────

        # 1. EMA crossover or sustained trend
        ema_ok = long_cross if direction == "long" else short_cross
        if not ema_ok:
            # sustained trend still counts but lower conviction
            ema_ok = trend_long if direction == "long" else trend_short

        # 2. VWAP side — price must be on correct side
        if direction == "long":
            vwap_ok = price > vwap
        else:
            vwap_ok = price < vwap

        # 3. Volume spike
        vol_ok = vol_mult >= vol_mult_req

        # 4. RSI confirmation
        if direction == "long":
            rsi_ok = rsi < rsi_ob      # not overbought
        else:
            rsi_ok = rsi > rsi_os      # not oversold

        # 5. Price action (higher highs + higher lows for long, opposite for short)
        if direction == "long":
            pa_ok = bool(row["hh"]) and bool(row["hl"])
        else:
            pa_ok = bool(row["lh"]) and bool(row["ll"])

        # ── Score ─────────────────────────────────────────────
        score = sum([ema_ok, vwap_ok, vol_ok, rsi_ok, pa_ok])

        # ── Calculate stops ───────────────────────────────────
        stop_mult = float(self._get_threshold("ATR_STOP_MULT", config.ATR_STOP_MULT))
        tp_mult   = float(self._get_threshold("ATR_TP_MULT",   config.ATR_TP_MULT))

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
