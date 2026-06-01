"""
risk/manager.py — Risk Manager v2.2

v2.2 changes (orphan fix):
  - manage_open_trades() now checks min_hold_seconds before firing any
    stop-loss or trailing stop logic. If a trade is younger than the
    symbol's min_hold_seconds, all exit logic is skipped for that cycle.
    This prevents orphan_404 trades where the bot tries to close a position
    before Alpaca has fully settled the order.
  - min_hold_seconds is read from symbol_profiles (set by the profiler).
    Falls back to config.MIN_HOLD_SECONDS (default 60) if no profile exists.

v2.1 changes:
  - Uses ENTRY-TIME ATR stored on the trade record for breakeven/trail/
    dynamic-TP calculations (not back-calculated from current ATR_TP_MULT).
"""

import time
import logging
from datetime import datetime, timezone
from core.database import (
    get_open_trades, get_open_trade_for_symbol,
    get_todays_pnl, get_todays_trade_count,
    set_breakeven, update_stop_loss, close_trade,
    get_config_override
)
import config
from core.notifier import notify_killed

log = logging.getLogger(__name__)

LOSS_DIRECTION_COOLDOWN = 20 * 60


class RiskManager:
    def __init__(self, starting_capital: float):
        self.starting_capital = starting_capital
        self.killed      = False
        self.kill_reason = None

        self._last_loss_direction = {}
        self._flip_exit_time      = {}

        # Dynamic TP state
        self._extended_tp    = {}   # trade_id -> extended_tp_price
        self._momentum_state = {}   # symbol -> (momentum_score, timestamp)

        log.info(f"[RISK] Risk manager initialized | Capital: ${starting_capital:,.2f}")

    # ── Momentum state (fed by main.py after each signal eval) ───────────────

    def update_momentum(self, symbol: str, momentum_score: int):
        self._momentum_state[symbol] = (momentum_score, time.time())

    # ── Daily kill switch ─────────────────────────────────────────────────────

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
            try:
                notify_killed(self.kill_reason, daily_pnl)
            except Exception:
                pass
            return False
        return True

    def reset_daily(self):
        self.killed = False
        self.kill_reason = None
        self._last_loss_direction = {}
        self._flip_exit_time = {}
        self._extended_tp    = {}
        self._momentum_state = {}
        log.info("[RISK] Daily reset ✓")

    # ── Direction flip tracking ───────────────────────────────────────────────

    def record_flip_exit(self, symbol: str):
        self._flip_exit_time[symbol] = time.time()
        log.info(f"[RISK] ↩️  FLIP EXIT {symbol} — waiting for reconfirmation")

    def flip_reconfirmed(self, symbol: str) -> bool:
        last_flip = self._flip_exit_time.get(symbol, 0)
        if last_flip == 0:
            return True
        elapsed = time.time() - last_flip
        if elapsed >= 90:
            del self._flip_exit_time[symbol]
            return True
        log.debug(f"[RISK] {symbol} waiting for flip reconfirmation — {int(90-elapsed)}s remaining")
        return False

    def should_flip_exit(self, symbol: str, new_direction: str) -> bool:
        existing = get_open_trade_for_symbol(symbol)
        if existing is None:
            return False
        if existing["side"] == new_direction:
            return False
        if existing["breakeven_set"]:
            log.debug(f"[RISK] {symbol} flip ignored — trade trailing, letting stop manage")
            return False
        return True

    # ── Trade validation ──────────────────────────────────────────────────────

    def can_trade(self, symbol, signal) -> tuple:
        if not self.check_daily_limits():
            return False, f"Daily loss limit: {self.kill_reason}"
        if signal.direction is None:
            return False, "No direction"
        key = (symbol, signal.direction)
        last_loss = self._last_loss_direction.get(key, 0)
        elapsed = time.time() - last_loss
        loss_cooldown = int(get_config_override("LOSS_COOLDOWN_MINS", getattr(config, "LOSS_COOLDOWN_MINS", 20))) * 60
        if elapsed < loss_cooldown:
            mins_left = int((loss_cooldown - elapsed) / 60)
            return False, f"Loss cooldown: {signal.direction} {symbol} blocked {mins_left}m"
        if not self.flip_reconfirmed(symbol):
            return False, f"Waiting for flip reconfirmation on {symbol}"
        open_trades = get_open_trades()
        max_trades  = int(get_config_override("MAX_OPEN_TRADES", config.MAX_OPEN_TRADES))
        if len(open_trades) >= max_trades:
            return False, f"Max open trades ({max_trades})"
        existing = get_open_trade_for_symbol(symbol)
        if existing:
            if existing["side"] != signal.direction:
                return False, f"Side conflict: have {existing['side']}, signal={signal.direction}"
            return False, f"Already open {symbol}"
        if signal.direction == "long":
            if signal.stop_loss >= signal.price:
                return False, "Stop >= entry for long"
            if signal.take_profit <= signal.price:
                return False, "TP <= entry for long"
        else:
            if signal.stop_loss <= signal.price:
                return False, "Stop <= entry for short"
            if signal.take_profit >= signal.price:
                return False, "TP >= entry for short"
        if signal.direction == "long":
            risk   = signal.price - signal.stop_loss
            reward = signal.take_profit - signal.price
        else:
            risk   = signal.stop_loss - signal.price
            reward = signal.price - signal.take_profit
        if risk <= 0:
            return False, "Risk zero or negative"
        rr = reward / risk
        min_rr = float(get_config_override("MIN_RR", getattr(config, "MIN_RR", 1.0)))
        if rr < min_rr:
            return False, f"R:R too low: {rr:.2f}:1 (need {min_rr:.1f}:1 min)"
        return True, None

    def record_loss(self, symbol, direction):
        key = (symbol, direction)
        self._last_loss_direction[key] = time.time()
        cooldown_mins = int(get_config_override("LOSS_COOLDOWN_MINS", getattr(config, "LOSS_COOLDOWN_MINS", 20)))
        log.info(f"[RISK] Loss recorded {direction} {symbol} — blocked {cooldown_mins}min")

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
        if signal.direction == "short":
            qty = max(int(qty), 1)
        else:
            qty = max(round(qty, 2), 0.01)
        log.info(
            f"[RISK] Position size {symbol}: qty={qty:.2f} "
            f"risk=${risk_per_share * qty:.2f} ({risk_pct*100:.0f}% of ${capital:.0f})"
        )
        return qty

    # ── Min-hold helper ───────────────────────────────────────────────────────

    def _get_min_hold_seconds(self, symbol: str) -> int:
        """
        Read per-symbol minimum hold time from symbol_profiles.
        Falls back to config.MIN_HOLD_SECONDS (global default: 60).
        The profiler sets this based on orphan history and volatility.
        """
        try:
            from core.database import get_conn
            import psycopg2.extras
            with get_conn() as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cur.execute(
                    "SELECT min_hold_seconds FROM symbol_profiles WHERE symbol = %s",
                    (symbol.upper(),)
                )
                row = cur.fetchone()
                if row and row["min_hold_seconds"] is not None:
                    return int(row["min_hold_seconds"])
        except Exception:
            pass
        return int(get_config_override(
            "MIN_HOLD_SECONDS", getattr(config, "MIN_HOLD_SECONDS", 60)
        ))

    # ── Active trade management ───────────────────────────────────────────────

    def manage_open_trades(self, current_prices: dict) -> list:
        actions     = []
        open_trades = get_open_trades()
        TRAIL_STEP  = float(get_config_override("TRAIL_STEP", getattr(config, "TRAIL_STEP", 0.5)))

        now_utc = datetime.now(timezone.utc)

        def _get_be_mult(symbol: str) -> float:
            try:
                from core.database import get_conn
                import psycopg2.extras
                with get_conn() as conn:
                    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                    cur.execute(
                        "SELECT breakeven_mult FROM symbol_profiles WHERE symbol = %s",
                        (symbol,)
                    )
                    row = cur.fetchone()
                    if row and row["breakeven_mult"] is not None:
                        return float(row["breakeven_mult"])
            except Exception:
                pass
            return float(get_config_override("BREAKEVEN_ATR_MULT", config.BREAKEVEN_ATR_MULT))

        # Dynamic TP config
        dynamic_tp_enabled = int(get_config_override("DYNAMIC_TP_ENABLED",     getattr(config, "DYNAMIC_TP_ENABLED",     1)))
        tp_extension       = float(get_config_override("DYNAMIC_TP_EXTENSION", getattr(config, "DYNAMIC_TP_EXTENSION",   1.0)))
        tp_min_momentum    = int(get_config_override("DYNAMIC_TP_MIN_MOMENTUM", getattr(config, "DYNAMIC_TP_MIN_MOMENTUM", 2)))

        for trade in open_trades:
            symbol   = trade["symbol"]
            price    = current_prices.get(symbol)
            if price is None:
                continue

            trade_id = trade["id"]
            side     = trade["side"]
            entry    = float(trade["entry_price"])
            stop     = float(trade["stop_loss"])
            tp       = float(trade["take_profit"])
            be_set   = trade["breakeven_set"]

            # ── Min-hold gate — prevents orphan_404 trades ────────────────────
            # Skip ALL stop/trail/TP logic if the trade is too young.
            # The profiler sets min_hold_seconds per symbol based on its
            # volatility and orphan history. Until this window expires,
            # Alpaca may not have the position fully registered.
            entered_at = trade.get("entered_at")
            if entered_at is not None:
                # entered_at is TIMESTAMPTZ — comes back from psycopg2 as
                # a timezone-aware datetime. Compare against UTC now.
                if entered_at.tzinfo is None:
                    # fallback: treat as naive UTC
                    entered_at = entered_at.replace(tzinfo=timezone.utc)
                elapsed_secs = (now_utc - entered_at).total_seconds()
                min_hold     = self._get_min_hold_seconds(symbol)
                if elapsed_secs < min_hold:
                    log.debug(
                        f"[RISK] {symbol} ({trade_id}) in min-hold window "
                        f"({elapsed_secs:.0f}s / {min_hold}s) — skipping stop logic"
                    )
                    continue

            # ── Determine entry-time ATR ──────────────────────────────────────
            # Prefer the ATR stored at trade entry (set via signal.atr).
            # Falls back to back-calc for old trades without stored ATR.
            stored_atr = trade.get("atr")
            if stored_atr is not None and float(stored_atr) > 0:
                estimated_atr = float(stored_atr)
            else:
                tp_mult = float(get_config_override("ATR_TP_MULT", config.ATR_TP_MULT))
                if tp_mult <= 0:
                    continue
                if side == "long":
                    estimated_atr = abs(tp - entry) / tp_mult
                else:
                    estimated_atr = abs(entry - tp) / tp_mult

            be_atr_mult    = _get_be_mult(symbol)
            be_trigger     = estimated_atr * be_atr_mult
            trail_distance = estimated_atr * TRAIL_STEP

            # ── Stop loss hit ─────────────────────────────────────────────────
            if side == "long" and price <= stop:
                reason = "trail_profit" if be_set else "stop"
                actions.append((trade_id, "close", price, reason, side))
                log.info(f"[RISK] 🔴 STOP {symbol} @ {price:.2f} (stop={stop:.2f})")
                self._extended_tp.pop(trade_id, None)
                continue

            if side == "short" and price >= stop:
                reason = "trail_profit" if be_set else "stop"
                actions.append((trade_id, "close", price, reason, side))
                log.info(f"[RISK] 🔴 STOP {symbol} @ {price:.2f} (stop={stop:.2f})")
                self._extended_tp.pop(trade_id, None)
                continue

            # ── Take profit hit ───────────────────────────────────────────────
            if side == "long" and price >= tp:
                extended = self._extended_tp.get(trade_id)
                if extended is not None:
                    if price < extended:
                        continue
                    self._extended_tp.pop(trade_id, None)
                    actions.append((trade_id, "close", price, "take_profit_extended", side))
                    log.info(f"[RISK] 🚀 TP EXTENDED HIT {symbol} @ {price:.2f}")
                    continue
                if dynamic_tp_enabled:
                    mom_state = self._momentum_state.get(symbol, (0, 0))
                    mom_score, mom_ts = mom_state
                    if mom_score >= tp_min_momentum and (time.time() - mom_ts) < 120:
                        new_tp = round(price + estimated_atr * tp_extension, 4)
                        self._extended_tp[trade_id] = new_tp
                        log.info(f"[RISK] 🚀 TP EXTENDED {symbol} {tp:.2f} → {new_tp:.2f} (momentum={mom_score}/3)")
                        continue
                actions.append((trade_id, "close", price, "take_profit", side))
                log.info(f"[RISK] 🟢 TP {symbol} @ {price:.2f} (tp={tp:.2f})")
                continue

            if side == "short" and price <= tp:
                extended = self._extended_tp.get(trade_id)
                if extended is not None:
                    if price > extended:
                        continue
                    self._extended_tp.pop(trade_id, None)
                    actions.append((trade_id, "close", price, "take_profit_extended", side))
                    log.info(f"[RISK] 🚀 TP EXTENDED HIT {symbol} @ {price:.2f}")
                    continue
                if dynamic_tp_enabled:
                    mom_state = self._momentum_state.get(symbol, (0, 0))
                    mom_score, mom_ts = mom_state
                    if mom_score >= tp_min_momentum and (time.time() - mom_ts) < 120:
                        new_tp = round(price - estimated_atr * tp_extension, 4)
                        self._extended_tp[trade_id] = new_tp
                        log.info(f"[RISK] 🚀 TP EXTENDED {symbol} {tp:.2f} → {new_tp:.2f} (momentum={mom_score}/3)")
                        continue
                actions.append((trade_id, "close", price, "take_profit", side))
                log.info(f"[RISK] 🟢 TP {symbol} @ {price:.2f} (tp={tp:.2f})")
                continue

            # ── ATR-based breakeven ───────────────────────────────────────────
            if not be_set:
                if side == "long" and price >= entry + be_trigger:
                    set_breakeven(trade_id, entry)
                    log.info(f"[RISK] ↗️  BREAKEVEN {symbol} — stop → entry {entry:.2f} (trigger={be_trigger:.2f})")
                elif side == "short" and price <= entry - be_trigger:
                    set_breakeven(trade_id, entry)
                    log.info(f"[RISK] ↗️  BREAKEVEN {symbol} — stop → entry {entry:.2f} (trigger={be_trigger:.2f})")

            # ── Trailing stop ─────────────────────────────────────────────────
            elif be_set:
                if side == "long":
                    new_stop = round(price - trail_distance, 4)
                    if new_stop > stop:
                        update_stop_loss(trade_id, new_stop)
                        log.info(f"[RISK] 📈 TRAIL {symbol} — stop {stop:.2f} → {new_stop:.2f}")
                elif side == "short":
                    new_stop = round(price + trail_distance, 4)
                    if new_stop < stop:
                        update_stop_loss(trade_id, new_stop)
                        log.info(f"[RISK] 📉 TRAIL {symbol} — stop {stop:.2f} → {new_stop:.2f}")

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
