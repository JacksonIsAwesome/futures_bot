# ============================================================
#   AlphaBot — Simulated Futures Trading Bot
#   Trades SPY/QQQ on Alpaca with 10x simulated leverage
#   to replicate micro futures (MNQ/MES) behavior
# ============================================================

import os

# ── Anthropic ────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ── Alpaca ───────────────────────────────────────────────────
ALPACA_API_KEY    = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")

# ── What we trade ────────────────────────────────────────────
# SPY  = S&P 500 ETF  → simulates MES (Micro E-mini S&P)
# QQQ  = Nasdaq ETF   → simulates MNQ (Micro E-mini Nasdaq)
# NVDA, AAPL          → high volume momentum plays
SYMBOLS = ["SPY", "QQQ", "NVDA", "AAPL"]
PRIMARY  = "QQQ"   # most volatile of the four

# ── Simulated futures leverage ────────────────────────────────
# Multiplies paper P&L to simulate futures leverage behavior.
# MNQ real leverage is roughly 10-20x. We use 10x to be conservative.
# Stops and position sizing all scale with this automatically.
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
ATR_STOP_MULT        = 1.5      # stop = entry - (ATR * 1.5)
ATR_TP_MULT          = 3.0      # take profit = entry + (ATR * 3.0)
BREAKEVEN_TRIGGER    = 10       # points profit before moving stop to entry
TRAIL_AFTER_BE       = True

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
