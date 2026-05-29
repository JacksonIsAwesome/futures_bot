# ============================================================
#   AlphaBot — Simulated Futures Trading Bot
#   Trades QQQ/NVDA on Alpaca with 10x simulated leverage
#   to replicate micro futures (MNQ/MES) behavior
# ============================================================

import os

# ── Anthropic ────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ── Alpaca ───────────────────────────────────────────────────
ALPACA_API_KEY    = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")

# ── What we trade ────────────────────────────────────────────
SYMBOLS = ["QQQ", "NVDA", "TQQQ", "SOXL"]
PRIMARY  = "QQQ"

# ── Simulated futures leverage ────────────────────────────────
SIMULATED_LEVERAGE = 10

# ── Capital ──────────────────────────────────────────────────
STARTING_CAPITAL     = 2000.0
MAX_DAILY_LOSS_PCT   = 0.30
MAX_OPEN_TRADES      = 7
MAX_POSITION_PCT     = 0.20
LOSS_COOLDOWN_MINS  = 20

# ── Strategy thresholds (meta brain can adjust) ───────────────
EMA_FAST             = 9
EMA_SLOW             = 21
VWAP_CONFIRM         = True
VOLUME_SPIKE_MULT    = 1.5
ROC_PERIOD           = 3
ROC_MIN_LONG         = 0.08
ROC_MIN_SHORT        = -0.08
VWAP_DEV_MULT        = 1.5
VOL_ACCEL_MULT       = 1.8
# Momentum gate
MOMENTUM_GATE_ENABLED       = 1
MOMENTUM_GATE_MIN           = 2
MACD_FAST                   = 12
MACD_SLOW                   = 26
MACD_SIGNAL_PERIOD          = 9
CANDLE_CONSISTENCY_LOOKBACK = 3
CANDLE_CONSISTENCY_MIN      = 2
# Multi-timeframe
MTF_FILTER_ENABLED          = 1
MTF_EMA_PERIOD              = 21
# Session aggression
PRIME_BASE_MIN              = 3
REGULAR_BASE_MIN            = 4
PRIME_END_HOUR              = 11
# Dynamic TP
DYNAMIC_TP_ENABLED          = 1
DYNAMIC_TP_EXTENSION        = 1.0
DYNAMIC_TP_MIN_MOMENTUM     = 2
# Faster scan
FAST_SCAN_ENABLED           = 1
FAST_SCAN_SCORE             = 5
FAST_SCAN_INTERVAL          = 20
# Direction flip
FLIP_ENABLED                = 1
FLIP_MIN_SIGNALS            = 1
FLIP_MIN_SIGNALS     = 1
FLIP_ENABLED         = 1
FLIP_MIN_SIGNALS     = 1   # signals needed in new direction before entering after a flip
RSI_PERIOD           = 14
RSI_OVERBOUGHT       = 70
RSI_OVERSOLD         = 30
MIN_SIGNAL_SCORE     = 3

# ── Risk / stops ─────────────────────────────────────────────
ATR_PERIOD           = 14
ATR_STOP_MULT        = 2.0
ATR_TP_MULT          = 4.0
TRAIL_AFTER_BE       = True

# ATR-based breakeven trigger.
# Stop moves to entry once price moves BREAKEVEN_ATR_MULT * ATR in our favor.
# 0.75 = price needs to move 75% of one ATR before stop locks to entry.
# Scales automatically per symbol — QQQ ATR ~$1.40 → trigger ~$1.05
#                                   NVDA ATR ~$1.50 → trigger ~$1.13
# Lower = faster breakeven, less profit needed but more premature exits
# Higher = slower breakeven, more room to breathe but more giveback risk
# Meta brain can adjust via DB override "BREAKEVEN_ATR_MULT"
BREAKEVEN_ATR_MULT   = 0.75

# ── Stream settings ───────────────────────────────────────────
STREAM_STALE_SECONDS = 120
MAX_BAR_AGE_MINUTES  = 20

# ── Scanning ─────────────────────────────────────────────────
SCAN_INTERVAL_SEC    = 5
MARKET_OPEN          = "09:30"
MARKET_CLOSE         = "16:00"
PRIME_OPEN_END       = "11:30"
PRIME_CLOSE_START    = "13:30"
TRADE_PRIME_ONLY     = False

# ── Meta Brain ───────────────────────────────────────────────
META_REVIEW_HOUR     = 21  # 5pm ET during EDT (UTC-4)
META_LOOKBACK_DAYS   = 7
META_MIN_TRADES      = 5
META_ADJUST_STEP     = 0.1

# ── Database ─────────────────────────────────────────────────
DATABASE_URL         = os.environ.get("DATABASE_URL", "")

# ── Logging ──────────────────────────────────────────────────
LOG_LEVEL            = "INFO"
