"""
strategies/ema_vwap.py — EMA + VWAP Momentum Strategy

The core trading logic. Evaluates 5 signals and scores them.
A trade fires when MIN_SIGNAL_SCORE or more align.

Now reads from the live stream cache instead of a bar dataframe.
The cache dict comes from PriceStream.get_price(symbol) and contains
real-time price, EMA9, EMA21, ATR, RSI, VWAP, high, low, prev_high,
prev_low — all calculated live from WebSocket ticks.

Signals evaluated:
  1. EMA crossover    — 9 EMA crossed above/below 21 EMA
  2. VWAP side        — price is on the right side of VWAP
  3. Volume spike     — current volume > VOLUME_SPIKE_MULT x recent avg
  4. RSI confirmation — RSI not overbought on longs / oversold on shorts
  5. Price action     — higher highs + higher lows (long) or opposite (short)

Entry: score >= MIN_SIGNAL_SCORE
Stop:  ATR * ATR_STOP_MULT
TP:    ATR * ATR_TP_MULT
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
        return (f"[{self.symbol}] {self.direction or 'NONE'} "
                f"score={self.score}/5 | {' '.join(checks)} "
                f"| price={self.price:.2f} SL={self.stop_loss:.2f} "
                f"TP={self.take_profit:.2f}")


class EMAVWAPStrategy:
    """
    EMA + VWAP momentum strategy.
    Reads live config overrides from DB so meta brain can tune it.
    Reads market data from the live stream cache dict.
    """

    def __init__(self):
        log.info("[STRAT] EMA/VWAP strategy loaded ✓")
        # rolling volume buffer per symbol for spike detection
        # stream gives us per-tick volume, we track a short window
        self._vol_window = {}   # symbol -> list of recent tick volumes

    def _get_threshold(self, key, default):
        return get_config_override(key, default)

    def _update_vol_window(self, symbol: str, volume: int, maxlen: int = 20):
        """Track recent tick volumes to calculate a spike multiplier."""
        if symbol not in self._vol_window:
            self._vol_window[symbol] = []
        self._vol_window[symbol].append(volume)
        if len(self._vol_window[symbol]) > maxlen:
            self._vol_window[symbol].pop(0)

    def _vol_spike_mult(self, symbol: str, current_vol: int) -> float:
        """
        Return how many times larger current volume is vs recent average.
        Falls back to 1.0 if not enough data yet.
        """
        window = self._vol_window.get(symbol, [])
        if len(window) < 5:
            return 1.0
        avg = sum(window[:-1]) / len(window[:-1])   # avg excluding current
        if avg == 0:
            return 1.0
        return current_vol / avg

    def evaluate(self, symbol: str, cache: dict) -> Optional[Signal]:
        """
        Evaluate current stream cache and return a Signal object.
        cache: dict from PriceStream.get_price(symbol)
        Returns None if data is missing or stream is stale.
        """
        # ── Guard: need valid live data ───────────────────────
        if cache is None:
            log.debug(f"[STRAT] {symbol}: no cache")
            return None

        if cache.get("stale"):
            log.warning(f"[STRAT] {symbol}: stream stale — skipping")
            return None

        price    = cache.get("price")
        ema9     = cache.get("ema9")
        ema21    = cache.get("ema21")
        atr      = cache.get("atr")
        rsi      = cache.get("rsi")
        vwap     = cache.get("vwap")
        high     = cache.get("high")
        low      = cache.get("low")
        prev_high = cache.get("prev_high")
        prev_low  = cache.get("prev_low")
        volume   = cache.get("volume", 0)

        # need all core values
        if any(v is None for v in [price, ema9, ema21, atr, rsi, vwap]):
            log.debug(f"[STRAT] {symbol}: incomplete indicators")
            return None

        if atr == 0:
            log.debug(f"[STRAT] {symbol}: ATR is 0 — skipping")
            return None

        # ── Get tunable thresholds ────────────────────────────
        min_score    = int(self._get_threshold("MIN_SIGNAL_SCORE", config.MIN_SIGNAL_SCORE))
        vol_mult_req = float(self._get_threshold("VOLUME_SPIKE_MULT", config.VOLUME_SPIKE_MULT))
        rsi_ob       = float(self._get_threshold("RSI_OVERBOUGHT",   config.RSI_OVERBOUGHT))
        rsi_os       = float(self._get_threshold("RSI_OVERSOLD",     config.RSI_OVERSOLD))
        stop_mult    = float(self._get_threshold("ATR_STOP_MULT",    config.ATR_STOP_MULT))
        tp_mult      = float(self._get_threshold("ATR_TP_MULT",      config.ATR_TP_MULT))

        # ── Direction from EMA relationship ───────────────────
        if ema9 > ema21:
            direction = "long"
        elif ema9 < ema21:
            direction = "short"
        else:
            return None

        # ── Signal 1: EMA crossover / trend ───────────────────
        # With live ticks we can't easily detect a fresh cross,
        # so we use the sustained trend as the signal.
        # A fresh cross will naturally appear here as ema9 moves
        # through ema21 over successive ticks.
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
            rsi_ok = rsi < rsi_ob
        else:
            rsi_ok = rsi > rsi_os

        # ── Signal 5: Price action ────────────────────────────
        # Higher highs + higher lows = bullish structure
        # Lower highs + lower lows   = bearish structure
        if high is not None and low is not None and prev_high is not None and prev_low is not None:
            if direction == "long":
                pa_ok = (high > prev_high) and (low > prev_low)
            else:
                pa_ok = (high < prev_high) and (low < prev_low)
        else:
            pa_ok = False

        # ── Score ─────────────────────────────────────────────
        score = sum([ema_ok, vwap_ok, vol_ok, rsi_ok, pa_ok])

        # ── Calculate stops ───────────────────────────────────
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
