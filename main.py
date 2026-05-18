"""
main.py — AlphaBot Main Loop

The brain of the operation. Runs continuously, scanning every 5 seconds.
Coordinates data → strategy → risk → execution in a clean loop.

Architecture:
  - FAST loop (5s): price check, stop/TP management
  - SLOW loop (60s): full indicator recalculation, new signal evaluation
  - DAILY loop (5pm ET): meta brain review, position close, daily reset
"""

import os
import time
import logging
import schedule
from datetime import datetime, date
import pytz

from config import (
    SYMBOLS, STARTING_CAPITAL, SCAN_INTERVAL_SEC,
    MARKET_OPEN, MARKET_CLOSE, META_REVIEW_HOUR
)
from core.database   import init_db, get_open_trades, upsert_daily_summary
from core.data       import DataFetcher
from core.execution  import ExecutionEngine
from risk.manager    import RiskManager
from strategies.ema_vwap import EMAVWAPStrategy
from meta.brain      import MetaBrain

# ── Logging setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")


class AlphaBot:
    def __init__(self):
        log.info("=" * 60)
        log.info("  AlphaBot — EMA/VWAP Momentum Strategy")
        log.info(f"  Capital: ${STARTING_CAPITAL:,.2f} | Symbols: {SYMBOLS}")
        log.info("=" * 60)

        init_db()

        self.data      = DataFetcher()
        self.execution = ExecutionEngine(self.data)
        self.risk      = RiskManager(STARTING_CAPITAL)
        self.strategy  = EMAVWAPStrategy()
        self.meta      = MetaBrain()

        # cache for full indicator dataframes (updated every 60s)
        self._df_cache    = {}
        self._last_slow   = {}   # symbol -> last slow-loop time
        self._scan_count  = 0
        self._last_date   = date.today()

        log.info("[BOOT] All modules initialized ✓")

    # ── Market hours ──────────────────────────────────────────

    def _is_market_open(self) -> bool:
        now = datetime.now(ET)
        if now.weekday() >= 5:   # weekend
            return False
        open_h,  open_m  = map(int, MARKET_OPEN.split(":"))
        close_h, close_m = map(int, MARKET_CLOSE.split(":"))
        market_open  = now.replace(hour=open_h,  minute=open_m,  second=0)
        market_close = now.replace(hour=close_h, minute=close_m, second=0)
        return market_open <= now < market_close

    def _is_end_of_day(self) -> bool:
        now = datetime.now(ET)
        close_h, close_m = map(int, MARKET_CLOSE.split(":"))
        eod = now.replace(hour=close_h - 1, minute=55, second=0)
        return now >= eod

    # ── Daily reset ───────────────────────────────────────────

    def _check_new_day(self):
        today = date.today()
        if today != self._last_date:
            log.info(f"[MAIN] New trading day: {today}")
            self.risk.reset_daily()
            self._last_date = today

    # ── Slow loop — full recalculation ────────────────────────

    def _run_slow_loop(self, symbol):
        """
        Runs every 60 seconds per symbol.
        Fetches full OHLCV, recalculates all indicators, evaluates signal.
        """
        df = self.data.get_full_snapshot(symbol)
        if df is not None:
            self._df_cache[symbol] = df

        df = self._df_cache.get(symbol)
        if df is None:
            return

        signal = self.strategy.evaluate(symbol, df)
        if signal is None:
            return

        # only attempt entry if signal has a direction
        if signal.direction is None:
            return

        # check risk rules
        ok, reason = self.risk.can_trade(symbol, signal)
        if not ok:
            log.debug(f"[MAIN] {symbol} blocked: {reason}")
            return

        # calculate position size
        qty = self.risk.calculate_position_size(symbol, signal)
        if qty <= 0:
            log.warning(f"[MAIN] {symbol} position size = 0, skipping")
            return

        # enter trade
        self.execution.enter_trade(signal, qty)

    # ── Fast loop — price checks and stop management ──────────

    def _run_fast_loop(self):
        """
        Runs every 5 seconds.
        Gets latest prices, checks stops/TPs, manages breakeven.
        """
        open_trades = get_open_trades()
        if not open_trades:
            return

        # get latest prices for all open symbols
        symbols_needed = list(set(t["symbol"] for t in open_trades))
        current_prices = {}
        for sym in symbols_needed:
            price = self.data.get_latest_price(sym)
            if price:
                current_prices[sym] = price

        if not current_prices:
            return

        # let risk manager check stops and breakeven
        actions = self.risk.manage_open_trades(current_prices)

        # execute any triggered actions
        for trade_id, action, price, reason in actions:
            if action == "close":
                # find the symbol for this trade
                trade = next((t for t in open_trades if t["id"] == trade_id), None)
                if trade:
                    self.execution.exit_trade(
                        trade_id, trade["symbol"], price, reason
                    )

    # ── Main scan cycle ───────────────────────────────────────

    def _scan(self):
        self._scan_count += 1
        self._check_new_day()

        # ── End of day close ──────────────────────────────────
        if self._is_end_of_day():
            open_trades = get_open_trades()
            if open_trades:
                log.info("[MAIN] 🔔 End of day — closing all positions")
                self.execution.close_all_positions(reason="eod")
                upsert_daily_summary()
            return

        # ── Always run fast loop ──────────────────────────────
        if self._is_market_open():
            self._run_fast_loop()

        # ── Slow loop every ~60 seconds per symbol ────────────
        now = time.time()
        for symbol in SYMBOLS:
            last = self._last_slow.get(symbol, 0)
            if now - last >= 60:
                if self._is_market_open():
                    self._run_slow_loop(symbol)
                self._last_slow[symbol] = now

        # ── Status log every 50 scans (~4 minutes) ───────────
        if self._scan_count % 50 == 0:
            open_trades  = get_open_trades()
            market_state = "OPEN" if self._is_market_open() else "CLOSED"
            log.info(
                f"[SCAN #{self._scan_count}] Market={market_state} | "
                f"Open positions={len(open_trades)} | "
                f"{self.risk.status()}"
            )

    # ── Daily meta brain review ───────────────────────────────

    def _daily_review(self):
        log.info("[MAIN] Running daily meta brain review...")
        upsert_daily_summary()
        self.meta.run_review()

    # ── Run ───────────────────────────────────────────────────

    def run(self):
        log.info(f"[MAIN] Starting scan loop (every {SCAN_INTERVAL_SEC}s)")

        # schedule daily review at 5pm ET
        schedule.every().day.at(f"{META_REVIEW_HOUR:02d}:00").do(self._daily_review)

        while True:
            try:
                self._scan()
                schedule.run_pending()
                time.sleep(SCAN_INTERVAL_SEC)

            except KeyboardInterrupt:
                log.info("[MAIN] Shutting down...")
                self.execution.close_all_positions("shutdown")
                break

            except Exception as e:
                log.error(f"[MAIN] Scan error: {e}", exc_info=True)
                time.sleep(10)   # brief pause on error, then continue


if __name__ == "__main__":
    bot = AlphaBot()
    bot.run()
