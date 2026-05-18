"""
core/execution.py — Trade Execution via Tradovate API

Places market orders for MNQ/MES micro futures.
Same safety check as before: verifies DB side before every close
so phantom short/long mismatches are impossible.
"""

import logging
import requests
from datetime import datetime
from core.database import (
    open_trade, close_trade,
    get_open_trade_for_symbol, get_open_trades
)
import config

log = logging.getLogger(__name__)

DEMO_BASE_URL = "https://demo.tradovateapi.com/v1"


class ExecutionEngine:
    def __init__(self, data_fetcher):
        # share the authenticated session from DataFetcher
        # so we don't need to auth twice
        self._data = data_fetcher
        log.info("[EXEC] Execution engine initialized (paper/demo mode)")

    def _session(self):
        """Use the data fetcher's authenticated session."""
        self._data._ensure_token()
        return self._data._session

    def _get_contract_id(self, symbol: str) -> int:
        """Look up front-month contract ID."""
        try:
            r = self._session().get(
                f"{DEMO_BASE_URL}/contract/find",
                params={"name": symbol},
                timeout=10
            )
            r.raise_for_status()
            return r.json()["id"]
        except Exception as e:
            log.error(f"[EXEC] Contract lookup failed {symbol}: {e}")
            return None

    def enter_trade(self, signal, qty: float) -> str:
        """
        Open a new futures position.
        qty = number of contracts (usually 1-2 for $2k account)
        Returns trade_id on success, None on failure.
        """
        contract_id = self._get_contract_id(signal.symbol)
        if not contract_id:
            return None

        action = "Buy" if signal.direction == "long" else "Sell"

        try:
            order = {
                "accountSpec":     config.TRADOVATE_USERNAME,
                "accountId":       config.TRADOVATE_ACCOUNT_ID,
                "action":          action,
                "symbol":          signal.symbol,
                "orderQty":        int(qty),
                "orderType":       "Market",
                "isAutomated":     True
            }
            r = self._session().post(
                f"{DEMO_BASE_URL}/order/placeorder",
                json=order,
                timeout=10
            )
            r.raise_for_status()
            data       = r.json()
            fill_price = float(data.get("price", signal.price))

            trade_id = open_trade(
                symbol=signal.symbol,
                side=signal.direction,
                qty=float(int(qty)),
                entry_price=fill_price,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                signal_score=signal.score,
                signal_id=signal.signal_id
            )

            log.info(
                f"[EXEC] OPEN {signal.direction.upper()} {signal.symbol} "
                f"@ {fill_price:.2f} | contracts={int(qty)} | "
                f"SL={signal.stop_loss:.2f} TP={signal.take_profit:.2f} | "
                f"id={trade_id}"
            )
            return trade_id

        except Exception as e:
            log.error(f"[EXEC] Failed to enter {signal.symbol}: {e}")
            return None

    def exit_trade(self, trade_id: str, symbol: str,
                   current_price: float, reason: str):
        """
        Close an existing futures position.

        CRITICAL SAFETY CHECK: looks up actual side in DB before placing
        any order. Prevents the phantom short/long bug from QuantBot.
        """
        # verify trade exists in DB
        db_trade = get_open_trade_for_symbol(symbol)

        if db_trade is None:
            log.error(
                f"[EXEC] EXIT BLOCKED — no open DB trade for {symbol} "
                f"(requested id={trade_id}). Skipping."
            )
            return None

        if db_trade["id"] != trade_id:
            log.error(
                f"[EXEC] EXIT BLOCKED — id mismatch for {symbol}. "
                f"DB={db_trade['id']} requested={trade_id}. Skipping."
            )
            return None

        actual_side = db_trade["side"]
        qty         = int(db_trade["qty"])

        # to close a long we sell, to close a short we buy
        action = "Sell" if actual_side == "long" else "Buy"

        contract_id = self._get_contract_id(symbol)
        if not contract_id:
            return None

        try:
            order = {
                "accountSpec": config.TRADOVATE_USERNAME,
                "accountId":   config.TRADOVATE_ACCOUNT_ID,
                "action":      action,
                "symbol":      symbol,
                "orderQty":    qty,
                "orderType":   "Market",
                "isAutomated": True
            }
            r = self._session().post(
                f"{DEMO_BASE_URL}/order/placeorder",
                json=order,
                timeout=10
            )
            r.raise_for_status()
            data       = r.json()
            fill_price = float(data.get("price", current_price))

            pnl = close_trade(trade_id, fill_price, reason)

            emoji = "🟢" if pnl and pnl > 0 else "🔴"
            log.info(
                f"[EXEC] {emoji} CLOSE {actual_side.upper()} {symbol} "
                f"@ {fill_price:.2f} | P&L=${pnl:.2f} | reason={reason} | "
                f"id={trade_id}"
            )
            return pnl

        except Exception as e:
            log.error(f"[EXEC] Failed to exit {symbol}: {e}")
            return None

    def close_all_positions(self, reason="eod"):
        """Close everything — used at end of day."""
        open_trades = get_open_trades()
        for trade in open_trades:
            self.exit_trade(
                trade["id"],
                trade["symbol"],
                trade["entry_price"],
                reason
            )
        log.info(f"[EXEC] All positions closed ({reason})")
