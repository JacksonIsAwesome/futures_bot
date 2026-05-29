"""
core/execution.py — Trade Execution via Alpaca Paper API
"""

import logging
import requests
from core.database import (
    open_trade, close_trade,
    get_open_trade_for_symbol, get_open_trades
)
import config

log = logging.getLogger(__name__)

ALPACA_TRADE_URL = "https://paper-api.alpaca.markets/v2"


class ExecutionEngine:
    def __init__(self, data_fetcher):
        self._data    = data_fetcher
        self._session = requests.Session()
        self._session.headers.update({
            "APCA-API-KEY-ID":     config.ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": config.ALPACA_SECRET_KEY,
            "Content-Type":        "application/json"
        })
        log.info(
            f"[EXEC] Execution engine initialized | "
            f"paper mode | leverage={config.SIMULATED_LEVERAGE}x"
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
            trade_id = open_trade(
                symbol=signal.symbol,
                side=signal.direction,
                qty=qty,
                entry_price=fill_price,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                signal_score=signal.score,
                signal_id=signal.signal_id
            )
            exposure = fill_price * qty * config.SIMULATED_LEVERAGE
            log.info(
                f"[EXEC] 🟢 OPEN {signal.direction.upper()} {signal.symbol} "
                f"@ ${fill_price:.2f} | qty={qty:.2f} | "
                f"exposure=${exposure:.0f} ({config.SIMULATED_LEVERAGE}x) | "
                f"SL=${signal.stop_loss:.2f} TP=${signal.take_profit:.2f} | "
                f"id={trade_id}"
            )
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

        # ── cancel any pending orders for this symbol first ──────────────────
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
            log.warning(
                f"[EXEC] Could not cancel existing orders for {symbol}: "
                f"{cancel_err}"
            )

        # ── close the position on Alpaca ──────────────────────────────────────
        try:
            r = self._session.delete(
                f"{ALPACA_TRADE_URL}/positions/{symbol}",
                timeout=10
            )
        except Exception as e:
            log.error(f"[EXEC] Network error closing {symbol}: {e}")
            return None

        # ── guard: only update DB if Alpaca confirmed the close ───────────────
        if r.status_code == 404:
            log.warning(
                f"[EXEC] {symbol} not found on Alpaca (404) — "
                f"closing DB trade {trade_id} as orphan_404"
            )
            close_trade(trade_id, float(entry_price), "orphan_404")
            return 0.0

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

        leveraged_pnl = raw_pnl * config.SIMULATED_LEVERAGE
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
            f"leveraged={config.SIMULATED_LEVERAGE}x → ${leveraged_pnl:.2f} | "
            f"reason={reason} | id={trade_id}"
        )
        return leveraged_pnl

    def close_all_positions(self, reason="eod"):
        """Close all open positions — used at end of day or shutdown."""
        open_trades = get_open_trades()
        if not open_trades:
            return
        for trade in open_trades:
            symbol = trade["symbol"]
            current_price = float(trade["entry_price"])
            self.exit_trade(trade["id"], symbol, current_price, reason)
        log.info(f"[EXEC] All positions closed ({reason})")

    def get_account(self):
        """Returns Alpaca account info."""
        try:
            r = self._session.get(f"{ALPACA_TRADE_URL}/account", timeout=5)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.error(f"[EXEC] Account fetch failed: {e}")
            return None
