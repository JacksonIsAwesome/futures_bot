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
# QQQ = Nasdaq 100 ETF → simulates MNQ (Micro E-mini Nasdaq)
# Single symbol only — avoids PDT spreading across symbols
# and keeps IEX data feed fresh (high volume = fresh bars)
SYMBOLS = ["QQQ"]
PRIMARY  = "QQQ"

# ── Simulated futures leverage ────────────────────────────────
# Multiplies paper P&L to simulate futures leverage behavior.
# MNQ real leverage is roughly 10-20x. We use 10x to be conservative.
SIMULATED_LEVERAGE = 10

# ── Capital ──────────────────────────────────────────────────
STARTING_CAPITAL     = 2000.0
MAX_DAILY_LOSS_PCT   = 0.30     # 30% = $600 max daily loss
MAX_OPEN_TRADES      = 7
MAX_POSITION_PCT     = 0.20     # max 20% of capital per trade

# ── Strategy thresholds (meta brain can adjust) ───────────────
EMA_FAST             = 9
EMA_SLOW             = 21
VWAP_CONFIRM         = True
VOLUME_SPIKE_MULT    = 1.5
RSI_PERIOD           = 14
RSI_OVERBOUGHT       = 70
RSI_OVERSOLD         = 30
MIN_SIGNAL_SCORE     = 3        # out of 5 signals must align

# ── Risk / stops ─────────────────────────────────────────────
ATR_PERIOD           = 14
ATR_STOP_MULT        = 2.0      # widened from 1.5 → gives more room
                                 # IEX 15-min delay means price has moved
                                 # by the time we enter — wider stop needed
ATR_TP_MULT          = 4.0      # widened from 3.0 → keeps R:R at 2:1
BREAKEVEN_TRIGGER    = 10       # points profit before moving stop to entry
TRAIL_AFTER_BE       = True

# ── Data staleness ────────────────────────────────────────────
# IEX feed can serve stale bars during low volume periods.
# If the most recent bar is older than this, skip the symbol.
MAX_BAR_AGE_MINUTES  = 20       # IEX has 15-min delay so 20 gives buffer

# ── Scanning ─────────────────────────────────────────────────
SCAN_INTERVAL_SEC    = 5
MARKET_OPEN          = "09:30"  # ET
MARKET_CLOSE         = "16:00"  # ET
PRIME_OPEN_END       = "11:30"
PRIME_CLOSE_START    = "13:30"
TRADE_PRIME_ONLY     = False

# ── Meta Brain ───────────────────────────────────────────────
META_REVIEW_HOUR     = 17       # 5pm ET after market close
META_LOOKBACK_DAYS   = 7
META_MIN_TRADES      = 10
META_ADJUST_STEP     = 0.1

# ── Database ─────────────────────────────────────────────────
DATABASE_URL         = os.environ.get("DATABASE_URL", "")

# ── Logging ──────────────────────────────────────────────────
LOG_LEVEL            = "INFO"
