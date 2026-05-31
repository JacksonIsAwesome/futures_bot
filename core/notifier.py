"""
core/notifier.py — SMS Trade Alerts via Twilio

Sends a text when:
  - A trade opens (entry price, direction, symbol)
  - A trade closes (exit price, P&L, reason)
  - Daily kill switch triggers (loss limit hit)

Setup:
  Add these to Railway environment variables:
    TWILIO_ACCOUNT_SID  — from twilio.com console
    TWILIO_AUTH_TOKEN   — from twilio.com console
    TWILIO_FROM         — your Twilio phone number (+1XXXXXXXXXX)
    TWILIO_TO           — your personal number (+1XXXXXXXXXX)
"""

import os
import logging
import requests

log = logging.getLogger(__name__)

TWILIO_SID   = os.environ.get("TWILIO_ACCOUNT_SID",  "")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN",   "")
TWILIO_FROM  = os.environ.get("TWILIO_FROM",         "")
TWILIO_TO    = os.environ.get("TWILIO_TO",           "")

_enabled = all([TWILIO_SID, TWILIO_TOKEN, TWILIO_FROM, TWILIO_TO])


def _send(msg: str):
    if not _enabled:
        log.debug(f"[SMS] Twilio not configured — skipping: {msg[:60]}")
        return
    try:
        r = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
            auth=(TWILIO_SID, TWILIO_TOKEN),
            data={"From": TWILIO_FROM, "To": TWILIO_TO, "Body": msg},
            timeout=10
        )
        if r.status_code == 201:
            log.info(f"[SMS] Sent ✓")
        else:
            log.warning(f"[SMS] Failed {r.status_code}: {r.text[:100]}")
    except Exception as e:
        log.error(f"[SMS] Error: {e}")


def notify_entry(symbol: str, direction: str, price: float,
                 qty: float, stop: float, tp: float, score: int):
    emoji = "🟢" if direction == "long" else "🔴"
    side  = "LONG" if direction == "long" else "SHORT"
    msg = (
        f"{emoji} AlphaBot ENTRY\n"
        f"{symbol} {side} @ ${price:.2f}\n"
        f"Qty: {qty:.2f} | Score: {score}/5\n"
        f"SL: ${stop:.2f} | TP: ${tp:.2f}"
    )
    _send(msg)


def notify_exit(symbol: str, direction: str, entry: float,
                exit_price: float, pnl: float, reason: str, leverage: int):
    emoji   = "✅" if pnl > 0 else "❌"
    side    = "LONG" if direction == "long" else "SHORT"
    reasons = {
        "take_profit":          "TP hit",
        "take_profit_extended": "Extended TP hit",
        "stop":                 "Stopped out",
        "trail_profit":         "Trail stopped",
        "direction_flip":       "Signal flipped",
        "eod":                  "End of day",
        "shutdown":             "Shutdown",
        "orphan_404":           "Orphan closed",
    }
    reason_label = reasons.get(reason, reason)
    msg = (
        f"{emoji} AlphaBot EXIT\n"
        f"{symbol} {side} | {reason_label}\n"
        f"Entry: ${entry:.2f} → Exit: ${exit_price:.2f}\n"
        f"P&L: {'+'if pnl>0 else ''}{pnl:.2f} ({leverage}x leveraged)"
    )
    _send(msg)


def notify_killed(reason: str, daily_pnl: float):
    msg = (
        f"🔴 AlphaBot KILLED\n"
        f"{reason}\n"
        f"Daily P&L: ${daily_pnl:.2f}\n"
        f"Bot is done for the day."
    )
    _send(msg)


def notify_eod_summary(trades: int, wins: int, pnl: float):
    wr    = round(wins / trades * 100) if trades else 0
    emoji = "📈" if pnl > 0 else "📉"
    msg = (
        f"{emoji} AlphaBot EOD Summary\n"
        f"Trades: {trades} | Wins: {wins} ({wr}%)\n"
        f"Daily P&L: {'+'if pnl>0 else ''}{pnl:.2f}"
    )
    _send(msg)
