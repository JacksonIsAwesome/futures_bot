# ============================================================
#   AlphaBot — Micro Futures Day Trading Bot
#   Config — all tunable parameters in one place
# ============================================================

# ── Anthropic ────────────────────────────────────────────────
ANTHROPIC_API_KEY = "YOUR_ANTHROPIC_API_KEY"

# ── Tradovate (paper/demo account) ───────────────────────────
TRADOVATE_USERNAME   = "YOUR_TRADOVATE_USERNAME"
TRADOVATE_PASSWORD   = "YOUR_TRADOVATE_PASSWORD"
TRADOVATE_APP_ID     = "YOUR_APP_NAME"        # name you gave your app in Tradovate
TRADOVATE_CID        = "YOUR_CID"             # client ID from Tradovate API settings
TRADOVATE_SECRET     = "YOUR_SECRET"          # client secret from Tradovate API settings
TRADOVATE_ACCOUNT_ID = 0                      # your demo account ID (integer, find in Tradovate dashboard)

# ── What we trade ────────────────────────────────────────────
# MNQ = Micro E-mini Nasdaq  | 1 point = $2  | ~100-300 point daily range
# MES = Micro E-mini S&P 500 | 1 point = $5  | ~30-80 point daily range
SYMBOLS = ["MNQM5", "MESM5"]   # M5 = June 2025 contract (update quarterly)
PRIMARY  = "MNQM5"             # main symbol

# ── Capital ──────────────────────────────────────────────────
STARTING_CAPITAL     = 2000.0
MAX_DAILY_LOSS_PCT   = 0.30     # 30% = $600 max loss before shutdown
MAX_OPEN_TRADES      = 7
MAX_POSITION_PCT     = 0.20     # max 20% of capital per trade

# ── Strategy thresholds (meta brain can adjust these) ────────
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
ATR_TP_MULT          = 3.0      # take profit = entry + (ATR * 3.0) → 2:1 R:R
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
META_REVIEW_HOUR     = 17       # 5pm ET
META_LOOKBACK_DAYS   = 7
META_MIN_TRADES      = 10
META_ADJUST_STEP     = 0.1

# ── Database ─────────────────────────────────────────────────
DATABASE_URL         = "YOUR_POSTGRESQL_URL"

# ── Logging ──────────────────────────────────────────────────
LOG_LEVEL            = "INFO"
