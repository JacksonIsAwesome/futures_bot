# ============================================================
#   AlphaBot — Simulated Futures Trading Bot
# ============================================================

import os

# ── Anthropic ────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ── Alpaca ───────────────────────────────────────────────────
ALPACA_API_KEY    = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")

# ── What we trade ────────────────────────────────────────────
SYMBOLS = ["QQQ", "NVDA", "TQQQ", "SPY", "SOXL", "AMD", "TSLA"]
PRIMARY  = "QQQ"

# ── Simulated futures leverage ────────────────────────────────
SIMULATED_LEVERAGE = 10

# ── Capital ──────────────────────────────────────────────────
STARTING_CAPITAL     = 2000.0
MAX_DAILY_LOSS_PCT   = 0.30
MAX_OPEN_TRADES      = 7
MAX_POSITION_PCT     = 0.20
RISK_PER_TRADE       = 0.02   # fraction of capital to risk per trade (2%) — DB overridable
LOSS_COOLDOWN_MINS   = 20
MIN_RR               = 1.0

# ── Strategy thresholds ───────────────────────────────────────
EMA_FAST             = 9
EMA_SLOW             = 21
VWAP_CONFIRM         = True
VOLUME_SPIKE_MULT    = 1.5
VWAP_DEV_MULT        = 1.5
VOL_ACCEL_MULT       = 1.8
RSI_PERIOD           = 14
RSI_OVERBOUGHT       = 70
RSI_OVERSOLD         = 30
MIN_SIGNAL_SCORE     = 3

# ── Momentum gate ─────────────────────────────────────────────
MOMENTUM_GATE_ENABLED       = 1
MOMENTUM_GATE_MIN           = 1
MACD_FAST                   = 5
MACD_SLOW                   = 13
MACD_SIGNAL_PERIOD          = 9

# ── Multi-timeframe ───────────────────────────────────────────
MTF_FILTER_ENABLED          = 1
MTF_EMA_PERIOD              = 21

# ── Session aggression ────────────────────────────────────────
PRIME_BASE_MIN              = 3
REGULAR_BASE_MIN            = 3
PRIME_END_HOUR              = 11

# ── Dynamic TP ────────────────────────────────────────────────
DYNAMIC_TP_ENABLED          = 1
DYNAMIC_TP_EXTENSION        = 1.0
DYNAMIC_TP_MIN_MOMENTUM     = 2

# ── Faster scan ───────────────────────────────────────────────
FAST_SCAN_ENABLED           = 1
FAST_SCAN_SCORE             = 4
FAST_SCAN_INTERVAL          = 20

# ── ADX regime filter ─────────────────────────────────────────
ADX_MIN_THRESHOLD           = 20.0

# ── Direction flip ─────────────────────────────────────────────
FLIP_ENABLED                = 1
FLIP_MIN_SIGNALS            = 3
FLIP_BASE_SCORE_MIN         = 4
FLIP_COOLDOWN_SEC           = 600

# ── Risk / stops ─────────────────────────────────────────────
ATR_PERIOD           = 14
ATR_STOP_MULT        = 2.0
ATR_TP_MULT          = 4.0
TRAIL_AFTER_BE       = True
BREAKEVEN_ATR_MULT   = 0.75
TRAIL_STEP           = 1.0    # widened from 0.5 — gives winners more room

# ── Opus morning call ─────────────────────────────────────────
MORNING_CALL_ENABLED = 1      # run Opus 4.8 at market open for symbol bias
MORNING_CALL_HOUR    = 9      # ET hour to run (9 = just before open)
MORNING_CALL_MINUTE  = 25     # at 9:25am ET

# ── Stream settings ───────────────────────────────────────────
STREAM_STALE_SECONDS = 120
MAX_BAR_AGE_MINUTES  = 20

# ── Scanning ─────────────────────────────────────────────────
SCAN_INTERVAL_SEC    = 5
MARKET_OPEN          = "09:30"
MARKET_CLOSE         = "16:00"
TRADING_PAUSED       = 0
MIN_HOLD_SECONDS         = 60
MORNING_BLACKOUT_ENABLED = 0
MORNING_BLACKOUT_MINS    = 10

# ── Meta Brain ───────────────────────────────────────────────
META_REVIEW_HOUR     = 21
META_LOOKBACK_DAYS   = 7
META_MIN_TRADES      = 5
META_ADJUST_STEP     = 0.1

# ── Database ─────────────────────────────────────────────────
DATABASE_URL         = os.environ.get("DATABASE_URL", "")

# ── Logging ──────────────────────────────────────────────────
LOG_LEVEL            = "INFO"
