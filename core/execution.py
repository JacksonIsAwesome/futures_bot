"""
core/execution.py — Trade Execution via Alpaca Paper API

v3.2 changes (orphan fix):
  - exit_trade() now retries on Alpaca 404 with a 10-second wait before
    giving up and marking the trade as orphan_404. This handles the common
    case where a market order fills but Alpaca hasn't registered the position
    in /positions yet (especially common in the first few seconds after market
    open or for fractional share orders).
  - Two-attempt flow: first DELETE → if 404, wait 10s → second DELETE →
    only then mark orphan if still 404.

v3.1 changes:
  - SIMULATED_LEVERAGE read live via get_config_override (dashboard slider works)
  - enter_trade passes signal.atr to open_trade so risk manager uses entry ATR
  - data_fetcher param removed from __init__ (was unused)
"""

import time
import logging
import requests
from core.database import (
    open_trade, close_trade,
    get_open_trade_for_symbol, get_open_trades,
    get_config_override
)
import config
from core.notifier import notify_entry, notify_exit

log = logging.getLogger(__name__)

ALPACA_TRADE_URL = "https://paper-api.alpaca.markets/v2"


def _leverage() -> int:
    """Read SIMULATED_LEVERAGE live so the dashboard slider takes effect."""
    return int(get_config_override("SIMULATED_LEVERAGE", config.SIMULATED_LEVERAGE))


class ExecutionEngine:
    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({
            "APCA-API-KEY-ID":     config.ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": config.ALPACA_SECRET_KEY,
            "Content-Type":        "application/json"
        })
        log.info(
            f"[EXEC] Execution engine initialized | "
            f"paper mode | leverage={_leverage()}x"
        )

    def enter_trade(self, signal, qty: float) -> str:
        side = "buy" if signal.direction == "long" else "sell"
        order_payload = {
            "symbol":        signal.symbol,
            "qty":           str(round(qty, 2)),
            "side":          side,
            "type":          "market",
            "time_in_force": "day"
        }
        try:
            r = self._session.post(
                f"{ALPACA_TRADE_URL}/orders",
                json=order_payload,
                timeout=10
            )
            r.raise_for_status()
            data       = r.json()
            fill_price = float(data.get("filled_avg_price") or signal.price)
            leverage   = _leverage()
            trade_id = open_trade(
                symbol=signal.symbol,
                side=signal.direction,
                qty=qty,
                entry_price=fill_price,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                signal_score=signal.score,
                signal_id=signal.signal_id,
                atr=signal.atr,
            )
            exposure = fill_price * qty * leverage
            log.info(
                f"[EXEC] 🟢 OPEN {signal.direction.upper()} {signal.symbol} "
                f"@ ${fill_price:.2f} | qty={qty:.2f} | "
                f"exposure=${exposure:.0f} ({leverage}x) | "
                f"SL=${signal.stop_loss:.2f} TP=${signal.take_profit:.2f} | "
                f"id={trade_id}"
            )
            notify_entry(signal.symbol, signal.direction, fill_price,
                         qty, signal.stop_loss, signal.take_profit, signal.score)
            return trade_id
        except requests.exceptions.HTTPError as e:
            try:
                alpaca_error = r.json()
            except Exception:
                alpaca_error = r.text
            log.error(
                f"[EXEC] Failed to enter {signal.symbol} ({side} {qty:.2f} shares): "
                f"HTTP {r.status_code} — {alpaca_error}"
            )
            return None
        except Exception as e:
            log.error(f"[EXEC] Failed to enter {signal.symbol}: {e}")
            return None

    def _close_position_on_alpaca(self, symbol: str) -> requests.Response | None:
        """Single attempt to close position via Alpaca DELETE. Returns response or None on network error."""
        try:
            return self._session.delete(
                f"{ALPACA_TRADE_URL}/positions/{symbol}",
                timeout=10
            )
        except Exception as e:
            log.error(f"[EXEC] Network error closing {symbol}: {e}")
            return None

    def exit_trade(self, trade_id: str, symbol: str,
                   current_price: float, reason: str):
        db_trade = get_open_trade_for_symbol(symbol)
        if db_trade is None:
            log.error(
                f"[EXEC] EXIT BLOCKED — no open DB trade for {symbol} "
                f"(id={trade_id}). Skipping."
            )
            return None
        if db_trade["id"] != trade_id:
            log.error(
                f"[EXEC] EXIT BLOCKED — id mismatch for {symbol}. "
                f"DB={db_trade['id']} requested={trade_id}. Skipping."
            )
            return None

        actual_side = db_trade["side"]
        qty         = db_trade["qty"]
        entry_price = db_trade["entry_price"]

        # ── Cancel any pending orders for this symbol first ────────────────────
        try:
            cancel_r = self._session.delete(
                f"{ALPACA_TRADE_URL}/orders",
                params={"symbol": symbol},
                timeout=5
            )
            if cancel_r.status_code not in (200, 204, 207):
                log.warning(
                    f"[EXEC] Cancel orders for {symbol} "
                    f"returned {cancel_r.status_code}"
                )
            else:
                log.debug(f"[EXEC] Cancelled existing orders for {symbol}")
        except Exception as cancel_err:
            log.warning(f"[EXEC] Could not cancel existing orders for {symbol}: {cancel_err}")

        # ── Close position on Alpaca — with one retry on 404 ──────────────────
        # A 404 at the first attempt usually means the market order was placed
        # but hasn't settled into a position on Alpaca's side yet. This is
        # common in the first few seconds after open, especially for fractional
        # orders. Waiting 10 seconds and retrying handles this case.
        r = self._close_position_on_alpaca(symbol)
        if r is None:
            return None

        if r.status_code == 404:
            log.warning(
                f"[EXEC] {symbol} not found on Alpaca (404, attempt 1) — "
                f"waiting 10s and retrying..."
            )
            time.sleep(10)
            r = self._close_position_on_alpaca(symbol)
            if r is None:
                return None

            if r.status_code == 404:
                log.warning(
                    f"[EXEC] {symbol} still 404 after retry — "
                    f"closing DB trade {trade_id} as orphan_404"
                )
                close_trade(trade_id, float(entry_price), "orphan_404")
                return 0.0

        # ── Non-200/204 from Alpaca — don't update DB ─────────────────────────
        if r.status_code not in (200, 204):
            try:
                alpaca_error = r.json()
            except Exception:
                alpaca_error = r.text
            log.error(
                f"[EXEC] Alpaca rejected close for {symbol} — "
                f"status={r.status_code} body={alpaca_error} — "
                f"DB NOT updated (position still live on Alpaca)"
            )
            return None

        # ── Alpaca confirmed close — calculate P&L and update DB ──────────────
        try:
            data = r.json()
        except Exception:
            data = {}

        fill_price = float(data.get("filled_avg_price") or current_price)

        if actual_side == "long":
            raw_pnl = (fill_price - float(entry_price)) * qty
        else:
            raw_pnl = (float(entry_price) - fill_price) * qty

        leverage      = _leverage()
        leveraged_pnl = raw_pnl * leverage
        close_trade(trade_id, fill_price, reason)

        from core.database import get_conn
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE trades SET pnl_usd=%s, pnl_pct=%s WHERE id=%s",
                (
                    round(leveraged_pnl, 4),
                    round(leveraged_pnl / (float(entry_price) * qty) * 100, 4),
                    trade_id
                )
            )

        emoji = "🟢" if leveraged_pnl > 0 else "🔴"
        log.info(
            f"[EXEC] {emoji} CLOSE {actual_side.upper()} {symbol} "
            f"@ ${fill_price:.2f} | "
            f"raw=${raw_pnl:.2f} | "
            f"leveraged={leverage}x → ${leveraged_pnl:.2f} | "
            f"reason={reason} | id={trade_id}"
        )
        notify_exit(symbol, actual_side, float(entry_price), fill_price,
                    leveraged_pnl, reason, leverage)
        return leveraged_pnl

    def close_all_positions(self, reason="eod"):
        """Close all open positions — two-pass: DB trades first, then Alpaca orphan sweep."""
        open_trades = get_open_trades()
        if open_trades:
            for trade in open_trades:
                symbol = trade["symbol"]
                current_price = float(trade["entry_price"])
                self.exit_trade(trade["id"], symbol, current_price, reason)
            log.info(f"[EXEC] DB positions closed ({reason})")
        else:
            log.info(f"[EXEC] No open DB trades at EOD — checking Alpaca directly")

        # Safety net: nuke anything still open on Alpaca (orphaned positions)
        try:
            r = self._session.delete(
                f"{ALPACA_TRADE_URL}/positions",
                params={"cancel_orders": "true"},
                timeout=15
            )
            if r.status_code in (200, 204, 207):
                try:
                    closed = r.json()
                    if isinstance(closed, list) and closed:
                        log.info(f"[EXEC] Alpaca safety close — nuked {len(closed)} orphaned position(s)")
                    else:
                        log.info("[EXEC] Alpaca safety close — no orphaned positions found")
                except Exception:
                    log.info("[EXEC] Alpaca safety close completed")
            elif r.status_code == 404:
                log.info("[EXEC] Alpaca safety close — no open positions on Alpaca")
            else:
                log.warning(f"[EXEC] Alpaca safety close returned {r.status_code}")
        except Exception as e:
            log.error(f"[EXEC] Alpaca safety close failed: {e}")

    def get_account(self):
        try:
            r = self._session.get(f"{ALPACA_TRADE_URL}/account", timeout=5)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.error(f"[EXEC] Account fetch failed: {e}")
            return None
