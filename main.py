"""
main.py — AlphaBot Main Loop

The brain of the operation. Runs continuously, scanning every 5 seconds.
Coordinates stream → strategy → risk → execution in a clean loop.

Architecture:
  - STREAM: WebSocket real-time price feed (replaces bar polling)
  - FAST loop (5s): price check, stop/TP/breakeven/trail management
  - SLOW loop (60s): signal evaluation, direction flip detection
  - DAILY loop (5pm ET): meta brain review, position close, daily reset
"""

import time
import logging
import schedule
from datetime import datetime, date
import pytz

from config import (
    SYMBOLS, STARTING_CAPITAL, SCAN_INTERVAL_SEC,
    MARKET_OPEN, MARKET_CLOSE, META_REVIEW_HOUR
)
from core.database      import init_db, get_open_trades, get_open_trade_for_symbol, upsert_daily_summary, get_config_override
from core.data          import DataFetcher
from core.stream        import PriceStream
from core.execution     import ExecutionEngine
from risk.manager       import RiskManager
from strategies.ema_vwap import EMAVWAPStrategy
from meta.brain         import MetaBrain
from meta.symbol_profiler import SymbolProfiler

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
        self.stream    = PriceStream(symbols=SYMBOLS)
        self.execution = ExecutionEngine(self.data)
        self.risk      = RiskManager(STARTING_CAPITAL)
        self.strategy  = EMAVWAPStrategy()
        self.meta      = MetaBrain()
        self.profiler  = SymbolProfiler()

        self._last_slow  = {}
        self._last_exit  = {}
        self._last_flip  = {}
        self._scan_count = 0
        self.COOLDOWN_SEC = 30 * 60
        self._last_date  = date.today()
        self._eod_closed = False

        log.info("[DB] Database initialized ✓")
        log.info("[DATA] Alpaca data fetcher initialized ✓")
        log.info("[EXEC] Execution engine initialized | paper mode | leverage=10x")
        log.info(f"[RISK] Risk manager initialized | Capital: ${STARTING_CAPITAL:,.2f}")
        log.info("[STRAT] EMA/VWAP strategy loaded ✓")
        log.info("[META] Meta brain initialized ✓")

        self.stream.start()

        # run symbol profiler at startup so profiles are ready before first trade
        try:
            self.profiler.run()
        except Exception as e:
            log.error(f"[PROFILER] Startup profiling failed: {e}")

        # ── CHANGE 1: Seed volume acceleration tracker ────────
        # Give the strategy historical candle volumes immediately so it
        # has a baseline for projecting intra-candle volume from minute 1.
        for sym in SYMBOLS:
            vols = self.stream.get_candle_volumes(sym, n=20)
            if vols:
                self.strategy.seed_volume_history(sym, vols)
                log.info(f"[BOOT] Volume history seeded for {sym}: {len(vols)} candles")

        log.info("[BOOT] All modules initialized ✓")
        log.info(f"[MAIN] Starting scan loop (every {SCAN_INTERVAL_SEC}s)")

    # ── Market hours ──────────────────────────────────────────

    def _is_market_open(self) -> bool:
        now = datetime.now(ET)
        if now.weekday() >= 5:
            return False
        open_h,  open_m  = map(int, MARKET_OPEN.split(":"))
        close_h, close_m = map(int, MARKET_CLOSE.split(":"))
        market_open  = now.replace(hour=open_h,  minute=open_m,  second=0)
        market_close = now.replace(hour=close_h, minute=close_m, second=0)
        return market_open <= now < market_close

    def _is_end_of_day(self) -> bool:
        now = datetime.now(ET)
        if now.weekday() >= 5:
            return False
        eod = now.replace(hour=15, minute=55, second=0, microsecond=0)
        return now >= eod

    def _is_blackout(self) -> bool:
        """
        Returns True if trading is currently blocked by the time blackout window.
        Configurable from the dashboard — BLACKOUT_ENABLED, BLACKOUT_START, BLACKOUT_END.
        Example: 11:00–13:00 ET blocks the choppy midday dead zone.
        """
        enabled = int(get_config_override("BLACKOUT_ENABLED", 0))
        if not enabled:
            return False
        now   = datetime.now(ET)
        start = int(get_config_override("BLACKOUT_START", 11))
        end   = int(get_config_override("BLACKOUT_END",   13))
        if now.hour >= start and now.hour < end:
            log.debug(f"[MAIN] Blackout active ({start}:00–{end}:00 ET) — no new entries")
            return True
        return False

    # ── Daily reset ───────────────────────────────────────────

    def _check_new_day(self):
        today = date.today()
        if today != self._last_date:
            log.info(f"[MAIN] New trading day: {today}")
            self.risk.reset_daily()
            self._eod_closed = False
            self._last_date = today

    # ── Slow loop — signal evaluation ─────────────────────────

    def _run_slow_loop(self, symbol: str):
        """
        Runs every 60 seconds per symbol.

        Flow:
          1. Get live cache and candles from stream
          2. Feed tick to volume acceleration tracker
          3. Evaluate signal via strategy (with elapsed_seconds for vol projection)
          4. Check for direction flip
          5. Risk check and entry
        """
        cache = self.stream.get_price(symbol)

        if cache is None:
            log.warning(f"[STREAM] {symbol}: no cache available")
            return

        if cache["stale"]:
            updated = cache.get("updated_at")
            if updated:
                age = (datetime.utcnow() - updated).total_seconds()
                log.warning(
                    f"[DATA] {symbol} data is stale — "
                    f"last tick was {age:.1f}s ago "
                    f"(max allowed: 120s). Skipping."
                )
            else:
                log.warning(f"[DATA] {symbol} data is stale — no ticks yet. Skipping.")
            return

        # ── Blackout window — block new entries during dead zone ─
        if self._is_blackout():
            return

        # ── CHANGE 2a: Feed tick to volume acceleration tracker ──
        # Must happen every scan so the tracker stays current.
        if cache.get("volume") is not None:
            candle_minute = self.stream.get_candle_minute(symbol)
            if candle_minute is not None:
                self.strategy.on_tick(symbol, cache["volume"], candle_minute)

        # ── CHANGE 2b: Get elapsed seconds for vol projection ────
        elapsed = self.stream.get_elapsed_seconds(symbol)

        # ── CHANGE 2c: Pass more candles + elapsed_seconds ───────
        # 25 candles (was 10) gives ROC and VWAP std dev enough history.
        candles = self.stream.get_candles(symbol, n=25)
        signal  = self.strategy.evaluate(symbol, cache, candles, elapsed_seconds=elapsed)

        if signal is None:
            return

        if signal.direction is None:
            return

        # ── Direction flip detection ──────────────────────────
        if self.risk.should_flip_exit(symbol, signal.direction):
            existing = get_open_trade_for_symbol(symbol)
            if existing:
                log.info(
                    f"[MAIN] 🔄 DIRECTION FLIP {symbol} — "
                    f"exiting {existing['side']} "
                    f"(signal flipped to {signal.direction})"
                )
                self.execution.exit_trade(
                    existing["id"],
                    symbol,
                    cache["price"],
                    "direction_flip"
                )
                self.risk.record_flip_exit(symbol)
                self._last_exit[symbol] = time.time()
                self._last_flip[symbol] = time.time()
            return

        # ── Cooldown check ────────────────────────────────────
        last_flip = self._last_flip.get(symbol, 0)
        last_exit = self._last_exit.get(symbol, 0)
        flip_cooldown = 60
        if time.time() - last_flip < flip_cooldown:
            secs_left = int(flip_cooldown - (time.time() - last_flip))
            log.debug(f"[MAIN] {symbol} in post-flip cooldown — {secs_left}s remaining")
            return
        elif time.time() - last_exit < self.COOLDOWN_SEC:
            mins_left = int((self.COOLDOWN_SEC - (time.time() - last_exit)) / 60)
            log.debug(f"[MAIN] {symbol} in cooldown — {mins_left}m remaining")
            return

        # ── Risk validation ───────────────────────────────────
        ok, reason = self.risk.can_trade(symbol, signal)
        if not ok:
            log.debug(f"[MAIN] {symbol} blocked: {reason}")
            return

        # ── Position sizing and entry ─────────────────────────
        qty = self.risk.calculate_position_size(symbol, signal)
        if qty <= 0:
            log.warning(f"[MAIN] {symbol} position size = 0, skipping")
            return

        self.execution.enter_trade(signal, qty)

    # ── Fast loop — stop/TP/breakeven/trail management ────────

    def _run_fast_loop(self):
        """
        Runs every 5 seconds.
        Reads live prices from stream cache for open position management.
        """
        open_trades = get_open_trades()
        if not open_trades:
            return

        current_prices = {}
        for trade in open_trades:
            sym   = trade["symbol"]
            cache = self.stream.get_price(sym)
            if cache and not cache["stale"] and cache["price"]:
                current_prices[sym] = cache["price"]

        if not current_prices:
            return

        actions = self.risk.manage_open_trades(current_prices)

        for trade_id, action, price, reason, side in actions:
            if action == "close":
                trade = next((t for t in open_trades if t["id"] == trade_id), None)
                if trade:
                    self.execution.exit_trade(
                        trade_id, trade["symbol"], price, reason
                    )
                    self._last_exit[trade["symbol"]] = time.time()
                    if reason == "stop":
                        self.risk.record_loss(trade["symbol"], trade["side"])

    # ── Main scan cycle ───────────────────────────────────────

    def _scan(self):
        self._scan_count += 1
        self._check_new_day()

        # ── End of day close ──────────────────────────────────
        if self._is_end_of_day():
            if not self._eod_closed:
                close_eod = int(get_config_override("CLOSE_EOD", 1))
                if close_eod:
                    open_trades = get_open_trades()
                    if open_trades:
                        log.info("[MAIN] 🔔 End of day — closing all positions")
                        self.execution.close_all_positions(reason="eod")
                    upsert_daily_summary()
                else:
                    log.info("[MAIN] 🔔 End of day — CLOSE_EOD off, holding overnight")
                self._eod_closed = True
            return

        # ── Fast loop (every scan) ────────────────────────────
        if self._is_market_open():
            self._run_fast_loop()

        # ── Slow loop (every 60s per symbol, market open only) ─
        if self._is_market_open():
            now = time.time()
            for symbol in SYMBOLS:
                last = self._last_slow.get(symbol, 0)
                if now - last >= 60:
                    self._run_slow_loop(symbol)
                    self._last_slow[symbol] = now

        # ── Manual meta brain trigger ─────────────────────────
        if self._scan_count % 12 == 0:
            self._check_meta_flag()

        # ── Status log ────────────────────────────────────────
        log_interval = 50 if self._is_market_open() else 60
        if self._scan_count % log_interval == 0:
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
        try:
            self.profiler.run()
        except Exception as e:
            log.error(f"[PROFILER] Nightly profiling failed: {e}")

    def _check_meta_flag(self):
        try:
            from core.database import get_config_override, set_config_override
            flag = get_config_override("RUN_META_NOW", None)
            if flag == "true":
                log.info("[MAIN] Manual meta brain review requested from dashboard...")
                set_config_override("RUN_META_NOW", "false")
                upsert_daily_summary()
                self.meta.run_review()
        except Exception as e:
            log.error(f"[MAIN] Meta flag check failed: {e}")

    # ── Run ───────────────────────────────────────────────────

    def run(self):
        schedule.every().day.at(f"{META_REVIEW_HOUR:02d}:00").do(self._daily_review)

        while True:
            try:
                self._scan()
                schedule.run_pending()
                time.sleep(SCAN_INTERVAL_SEC)

            except KeyboardInterrupt:
                log.info("[MAIN] Shutting down...")
                self.stream.stop()
                self.execution.close_all_positions("shutdown")
                break

            except Exception as e:
                log.error(f"[MAIN] Scan error: {e}", exc_info=True)
                time.sleep(10)


if __name__ == "__main__":
    bot = AlphaBot()
    bot.run()
