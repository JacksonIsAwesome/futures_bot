# ============================================================
#   AlphaBot — Simulated Futures Trading Bot
#   Trades QQQ on Alpaca with 10x simulated leverage
#   to replicate micro futures (MNQ) behavior
#
#   QQQ only — most volatile, cleanest IEX data feed,
#   least PDT friction of the available symbols
# ============================================================

import os

# ── Anthropic ────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ── Alpaca ───────────────────────────────────────────────────
ALPACA_API_KEY    = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")

# ── What we trade ────────────────────────────────────────────
SYMBOLS  = ["QQQ", "NVDA"]
PRIMARY  = "QQQ"

# ── Simulated futures leverage ────────────────────────────────
SIMULATED_LEVERAGE = 10

# ── Capital ──────────────────────────────────────────────────
STARTING_CAPITAL     = 2000.0
MAX_DAILY_LOSS_PCT   = 0.30
MAX_OPEN_TRADES      = 7
MAX_POSITION_PCT     = 0.20

# ── Strategy thresholds (meta brain can adjust) ───────────────
EMA_FAST             = 9
EMA_SLOW             = 21
VWAP_CONFIRM         = True
VOLUME_SPIKE_MULT    = 1.5
RSI_PERIOD           = 14
RSI_OVERBOUGHT       = 70
RSI_OVERSOLD         = 30
MIN_SIGNAL_SCORE     = 3

# ── Risk / stops ─────────────────────────────────────────────
ATR_PERIOD           = 14
ATR_STOP_MULT        = 2.0
ATR_TP_MULT          = 4.0
BREAKEVEN_TRIGGER    = 10
TRAIL_AFTER_BE       = True

# ── Stream settings ───────────────────────────────────────────
# WebSocket real-time feed replaces bar polling.
# STALE_SECONDS: mark symbol stale if no tick received in this window.
# During low volume periods ticks may slow — 120s is safe.
STREAM_STALE_SECONDS = 120

# ── Data staleness (kept for fallback reference) ──────────────
MAX_BAR_AGE_MINUTES  = 20

# ── Scanning ─────────────────────────────────────────────────
SCAN_INTERVAL_SEC    = 5
MARKET_OPEN          = "09:30"
MARKET_CLOSE         = "16:00"
PRIME_OPEN_END       = "11:30"
PRIME_CLOSE_START    = "13:30"
TRADE_PRIME_ONLY     = False

# ── Meta Brain ───────────────────────────────────────────────
META_REVIEW_HOUR     = 17
META_LOOKBACK_DAYS   = 7
META_MIN_TRADES      = 5
META_ADJUST_STEP     = 0.1

# ── Database ─────────────────────────────────────────────────
DATABASE_URL         = os.environ.get("DATABASE_URL", "")

# ── Logging ──────────────────────────────────────────────────
LOG_LEVEL            = "INFO"
