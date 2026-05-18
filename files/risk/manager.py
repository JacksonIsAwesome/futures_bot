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
"""

import logging
from core.database import (
    get_open_trades, get_open_trade_for_symbol,
    get_todays_pnl, get_todays_trade_count,
    set_breakeven, close_trade, get_config_override
)
import config

log = logging.getLogger(__name__)


class RiskManager:
    def __init__(self, starting_capital: float):
        self.starting_capital = starting_capital
        self.killed = False
        self.kill_reason = None
        log.info(f"[RISK] Risk manager initialized | Capital: ${starting_capital:,.2f}")

    # ── Daily kill switch ─────────────────────────────────────

    def check_daily_limits(self) -> bool:
        """
        Returns True if trading is allowed.
        Returns False and sets killed=True if daily loss limit hit.
        """
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
        """Call at start of each new trading day."""
        self.killed = False
        self.kill_reason = None
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

        # 3. Max open trades
        open_trades = get_open_trades()
        max_trades  = int(get_config_override("MAX_OPEN_TRADES", config.MAX_OPEN_TRADES))
        if len(open_trades) >= max_trades:
            return False, f"Max open trades reached ({max_trades})"

        # 4. No duplicate symbol positions
        existing = get_open_trade_for_symbol(symbol)
        if existing:
            # CRITICAL: verify side matches before doing anything
            if existing["side"] != signal.direction:
                return False, (
                    f"Side conflict: have open {existing['side']} "
                    f"but signal says {signal.direction}"
                )
            return False, f"Already have open {symbol} position"

        # 5. Validate stop/TP math
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

        # 6. Minimum R:R ratio (1.5:1 minimum)
        if signal.direction == "long":
            risk   = signal.price - signal.stop_loss
            reward = signal.take_profit - signal.price
        else:
            risk   = signal.stop_loss - signal.price
            reward = signal.price - signal.take_profit

        if risk <= 0 or (reward / risk) < 1.5:
            return False, f"R:R too low: {reward/risk:.2f}:1 (need 1.5:1 min)"

        return True, None

    def calculate_position_size(self, symbol, signal) -> float:
        """
        Calculate how many shares/contracts to buy.
        Sizes so max loss on this trade = 2% of capital.
        Never exceeds MAX_POSITION_PCT of capital.
        """
        max_pct   = float(get_config_override("MAX_POSITION_PCT", config.MAX_POSITION_PCT))
        risk_pct  = 0.02   # risk 2% of capital per trade

        price     = signal.price
        stop      = signal.stop_loss
        risk_per_share = abs(price - stop)

        if risk_per_share <= 0:
            return 0

        # size by risk
        capital    = self.starting_capital
        risk_budget = capital * risk_pct
        qty_by_risk = risk_budget / risk_per_share

        # size by max position
        max_position_value = capital * max_pct
        qty_by_size = max_position_value / price

        # take the smaller of the two
        qty = min(qty_by_risk, qty_by_size)
        qty = max(round(qty, 2), 0.01)   # minimum 0.01 shares (fractional)

        log.info(
            f"[RISK] Position size {symbol}: qty={qty:.2f} "
            f"risk=${risk_per_share * qty:.2f} "
            f"({risk_pct*100:.0f}% of ${capital:.0f})"
        )
        return qty

    # ── Active trade management ───────────────────────────────

    def manage_open_trades(self, current_prices: dict) -> list:
        """
        Called every scan cycle.
        Checks open trades for:
          - Stop loss hit
          - Take profit hit
          - Breakeven trigger
        Returns list of (trade_id, action, price, reason) to execute.
        """
        actions = []
        open_trades = get_open_trades()

        for trade in open_trades:
            symbol = trade["symbol"]
            price  = current_prices.get(symbol)

            if price is None:
                continue

            trade_id    = trade["id"]
            side        = trade["side"]
            entry       = trade["entry_price"]
            stop        = trade["stop_loss"]
            tp          = trade["take_profit"]
            be_set      = trade["breakeven_set"]
            be_trigger  = config.BREAKEVEN_TRIGGER

            # ── Stop loss hit ─────────────────────────────────
            if side == "long" and price <= stop:
                actions.append((trade_id, "close", price, "stop"))
                log.info(f"[RISK] 🔴 STOP {symbol} @ {price:.2f} (stop={stop:.2f})")
                continue

            if side == "short" and price >= stop:
                actions.append((trade_id, "close", price, "stop"))
                log.info(f"[RISK] 🔴 STOP {symbol} @ {price:.2f} (stop={stop:.2f})")
                continue

            # ── Take profit hit ───────────────────────────────
            if side == "long" and price >= tp:
                actions.append((trade_id, "close", price, "take_profit"))
                log.info(f"[RISK] 🟢 TP {symbol} @ {price:.2f} (tp={tp:.2f})")
                continue

            if side == "short" and price <= tp:
                actions.append((trade_id, "close", price, "take_profit"))
                log.info(f"[RISK] 🟢 TP {symbol} @ {price:.2f} (tp={tp:.2f})")
                continue

            # ── Move to breakeven ─────────────────────────────
            if not be_set:
                if side == "long" and price >= entry + be_trigger:
                    set_breakeven(trade_id, entry)
                    log.info(f"[RISK] ↗️  BREAKEVEN {symbol} stop moved to {entry:.2f}")

                if side == "short" and price <= entry - be_trigger:
                    set_breakeven(trade_id, entry)
                    log.info(f"[RISK] ↗️  BREAKEVEN {symbol} stop moved to {entry:.2f}")

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
