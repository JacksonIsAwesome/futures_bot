"""
risk/manager.py — Risk Manager

Controls position sizing, daily loss limits, and trade validation.
This is the gatekeeper — nothing gets traded without passing here.

Rules:
  - Max 30% daily loss before shutdown
  - Max 7 open trades at once
  - Max 20% of capital per trade
  - Never trades the same symbol twice simultaneously
  - Validates stop/TP math before entry
  - Moves stop to breakeven once trade is profitable enough
  - Trails stop after breakeven to lock in gains
  - Same-direction loss cooldown — after a loss, waits before
    entering the same direction again on the same symbol
"""

import time
import logging
from core.database import (
    get_open_trades, get_open_trade_for_symbol,
    get_todays_pnl, get_todays_trade_count,
    set_breakeven, update_stop_loss, close_trade,
    get_config_override
)
import config

log = logging.getLogger(__name__)

# After a losing trade, wait this long before re-entering
# the same direction on the same symbol (in seconds)
LOSS_DIRECTION_COOLDOWN = 20 * 60   # 20 minutes


class RiskManager:
    def __init__(self, starting_capital: float):
        self.starting_capital = starting_capital
        self.killed      = False
        self.kill_reason = None

        # tracks (symbol, direction) -> timestamp of last loss
        self._last_loss_direction = {}

        log.info(f"[RISK] Risk manager initialized | Capital: ${starting_capital:,.2f}")

    # ── Daily kill switch ─────────────────────────────────────

    def check_daily_limits(self) -> bool:
        if self.killed:
            return False

        daily_pnl = get_todays_pnl()
        max_loss  = self.starting_capital * get_config_override(
            "MAX_DAILY_LOSS_PCT", config.MAX_DAILY_LOSS_PCT
        )

        if daily_pnl <= -abs(max_loss):
            self.killed = True
            self.kill_reason = f"Daily loss limit hit: ${daily_pnl:.2f}"
            log.warning(f"[RISK] 🔴 KILLED: {self.kill_reason}")
            return False

        return True

    def reset_daily(self):
        self.killed = False
        self.kill_reason = None
        self._last_loss_direction = {}
        log.info("[RISK] Daily reset ✓")

    # ── Trade validation ──────────────────────────────────────

    def can_trade(self, symbol, signal) -> tuple:
        """
        Returns (True, None) if trade is allowed.
        Returns (False, reason) if blocked.
        """
        # 1. Kill switch
        if not self.check_daily_limits():
            return False, f"Daily loss limit: {self.kill_reason}"

        # 2. Signal must have a direction
        if signal.direction is None:
            return False, "No direction in signal"

        # 3. Same-direction loss cooldown
        key = (symbol, signal.direction)
        last_loss = self._last_loss_direction.get(key, 0)
        elapsed = time.time() - last_loss
        if elapsed < LOSS_DIRECTION_COOLDOWN:
            mins_left = int((LOSS_DIRECTION_COOLDOWN - elapsed) / 60)
            return False, (
                f"Loss cooldown: {signal.direction} {symbol} "
                f"blocked for {mins_left}m after last loss"
            )

        # 4. Max open trades
        open_trades = get_open_trades()
        max_trades  = int(get_config_override("MAX_OPEN_TRADES", config.MAX_OPEN_TRADES))
        if len(open_trades) >= max_trades:
            return False, f"Max open trades reached ({max_trades})"

        # 5. No duplicate symbol positions
        existing = get_open_trade_for_symbol(symbol)
        if existing:
            if existing["side"] != signal.direction:
                return False, (
                    f"Side conflict: have open {existing['side']} "
                    f"but signal says {signal.direction}"
                )
            return False, f"Already have open {symbol} position"

        # 6. Validate stop/TP math
        if signal.direction == "long":
            if signal.stop_loss >= signal.price:
                return False, "Stop loss must be below entry for longs"
            if signal.take_profit <= signal.price:
                return False, "Take profit must be above entry for longs"
        else:
            if signal.stop_loss <= signal.price:
                return False, "Stop loss must be above entry for shorts"
            if signal.take_profit >= signal.price:
                return False, "Take profit must be below entry for shorts"

        # 7. Minimum R:R ratio
        # Lowered from 1.5 to 1.2 — live tick ATR is smaller than bar ATR
        # so stops/TPs are tighter, making 1.5 too strict for live data.
        # 1.2:1 still ensures positive expectancy while allowing more trades.
        if signal.direction == "long":
            risk   = signal.price - signal.stop_loss
            reward = signal.take_profit - signal.price
        else:
            risk   = signal.stop_loss - signal.price
            reward = signal.price - signal.take_profit

        if risk <= 0:
            return False, "Risk is zero or negative"

        rr = reward / risk
        if rr < 1.2:
            return False, f"R:R too low: {rr:.2f}:1 (need 1.2:1 min)"

        return True, None

    def record_loss(self, symbol, direction):
        key = (symbol, direction)
        self._last_loss_direction[key] = time.time()
        log.info(
            f"[RISK] Loss recorded for {direction} {symbol} — "
            f"same direction blocked for {LOSS_DIRECTION_COOLDOWN // 60} min"
        )

    def calculate_position_size(self, symbol, signal) -> float:
        max_pct        = float(get_config_override("MAX_POSITION_PCT", config.MAX_POSITION_PCT))
        risk_pct       = 0.02

        price          = signal.price
        stop           = signal.stop_loss
        risk_per_share = abs(price - stop)

        if risk_per_share <= 0:
            return 0

        capital            = self.starting_capital
        risk_budget        = capital * risk_pct
        qty_by_risk        = risk_budget / risk_per_share
        max_position_value = capital * max_pct
        qty_by_size        = max_position_value / price

        qty = min(qty_by_risk, qty_by_size)
        qty = max(round(qty, 2), 0.01)

        log.info(
            f"[RISK] Position size {symbol}: qty={qty:.2f} "
            f"risk=${risk_per_share * qty:.2f} "
            f"({risk_pct*100:.0f}% of ${capital:.0f})"
        )
        return qty

    # ── Active trade management ───────────────────────────────

    def manage_open_trades(self, current_prices: dict) -> list:
        """
        Called every 5-second scan cycle.
        Returns list of (trade_id, action, price, reason, side) to execute.
        """
        actions     = []
        open_trades = get_open_trades()

        TRAIL_STEP = 0.5

        for trade in open_trades:
            symbol = trade["symbol"]
            price  = current_prices.get(symbol)

            if price is None:
                continue

            trade_id   = trade["id"]
            side       = trade["side"]
            entry      = float(trade["entry_price"])
            stop       = float(trade["stop_loss"])
            tp         = float(trade["take_profit"])
            be_set     = trade["breakeven_set"]
            be_trigger = config.BREAKEVEN_TRIGGER

            tp_mult = get_config_override("ATR_TP_MULT", config.ATR_TP_MULT)
            if side == "long":
                estimated_atr = abs(tp - entry) / tp_mult
            else:
                estimated_atr = abs(entry - tp) / tp_mult
            trail_distance = estimated_atr * TRAIL_STEP

            # ── Stop loss hit ─────────────────────────────────
            if side == "long" and price <= stop:
                actions.append((trade_id, "close", price, "stop", side))
                log.info(f"[RISK] 🔴 STOP {symbol} @ {price:.2f} (stop={stop:.2f})")
                continue

            if side == "short" and price >= stop:
                actions.append((trade_id, "close", price, "stop", side))
                log.info(f"[RISK] 🔴 STOP {symbol} @ {price:.2f} (stop={stop:.2f})")
                continue

            # ── Take profit hit ───────────────────────────────
            if side == "long" and price >= tp:
                actions.append((trade_id, "close", price, "take_profit", side))
                log.info(f"[RISK] 🟢 TP {symbol} @ {price:.2f} (tp={tp:.2f})")
                continue

            if side == "short" and price <= tp:
                actions.append((trade_id, "close", price, "take_profit", side))
                log.info(f"[RISK] 🟢 TP {symbol} @ {price:.2f} (tp={tp:.2f})")
                continue

            # ── Move to breakeven ─────────────────────────────
            if not be_set:
                if side == "long" and price >= entry + be_trigger:
                    set_breakeven(trade_id, entry)
                    log.info(
                        f"[RISK] ↗️  BREAKEVEN {symbol} — "
                        f"stop moved to entry {entry:.2f}"
                    )

                if side == "short" and price <= entry - be_trigger:
                    set_breakeven(trade_id, entry)
                    log.info(
                        f"[RISK] ↗️  BREAKEVEN {symbol} — "
                        f"stop moved to entry {entry:.2f}"
                    )

            # ── Trailing stop (only after breakeven is set) ───
            elif be_set:
                if side == "long":
                    new_stop = round(price - trail_distance, 4)
                    if new_stop > stop:
                        update_stop_loss(trade_id, new_stop)
                        log.info(
                            f"[RISK] 📈 TRAIL {symbol} — "
                            f"stop {stop:.2f} → {new_stop:.2f} "
                            f"(price={price:.2f})"
                        )

                elif side == "short":
                    new_stop = round(price + trail_distance, 4)
                    if new_stop < stop:
                        update_stop_loss(trade_id, new_stop)
                        log.info(
                            f"[RISK] 📉 TRAIL {symbol} — "
                            f"stop {stop:.2f} → {new_stop:.2f} "
                            f"(price={price:.2f})"
                        )

        return actions

    def status(self) -> str:
        open_trades = get_open_trades()
        daily_pnl   = get_todays_pnl()
        max_loss    = self.starting_capital * config.MAX_DAILY_LOSS_PCT
        return (
            f"Open={len(open_trades)}/{config.MAX_OPEN_TRADES} | "
            f"DailyP&L=${daily_pnl:.2f} | "
            f"Limit=${-max_loss:.2f} | "
            f"{'⛔ KILLED' if self.killed else '✓ Active'}"
        )
