"""
meta/symbol_profiler.py — Per-Symbol AI Intelligence Layer

v2 fix: _save_profile now enforces minimum ATR floors for leveraged ETFs.
Claude's suggested min_atr_pct is overridden to at least 1.0% for any 3x ETF.
This prevents stops from being placed too tight on high-volatility instruments
regardless of what Claude returns.
"""

import json
import logging
import requests
import psycopg2.extras
from datetime import datetime, date, timedelta
from core.database import get_conn
import config

log = logging.getLogger(__name__)

BARS_URL = "https://data.alpaca.markets/v2/stocks/{symbol}/bars"

# Symbols that are leveraged ETFs — enforce stricter ATR floors
LEVERAGED_ETFS = {"TQQQ", "SOXL", "SPXL", "UPRO", "UDOW", "LABU", "FNGU"}
LEVERAGED_ETF_MIN_ATR_PCT = 0.010   # 1.0% minimum for 3x ETFs
NORMAL_MIN_ATR_PCT        = 0.003   # 0.3% minimum for everything else


class SymbolProfiler:
    def __init__(self):
        self._ensure_table()
        log.info("[PROFILER] Symbol profiler initialized ✓")

    def _ensure_table(self):
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS symbol_profiles (
                    symbol            TEXT PRIMARY KEY,
                    atr_stop_mult     FLOAT,
                    atr_tp_mult       FLOAT,
                    breakeven_mult    FLOAT,
                    volume_spike_mult FLOAT,
                    min_atr_floor     FLOAT,
                    min_atr_pct       FLOAT,
                    notes             TEXT,
                    raw_response      TEXT,
                    updated_at        TIMESTAMP DEFAULT NOW()
                )
            """)

    def run(self):
        log.info("[PROFILER] ═══════════════════════════════════════════")
        log.info("[PROFILER] Starting symbol profile update...")
        for symbol in config.SYMBOLS:
            try:
                self._profile_symbol(symbol)
            except Exception as e:
                log.error(f"[PROFILER] Failed to profile {symbol}: {e}")
        log.info("[PROFILER] Symbol profiles updated ✓")
        log.info("[PROFILER] ═══════════════════════════════════════════")

    def get_profile(self, symbol: str) -> dict:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(
                "SELECT * FROM symbol_profiles WHERE symbol = %s",
                (symbol.upper(),)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _fetch_bars(self, symbol: str, days: int = 30) -> list:
        end   = datetime.utcnow()
        start = end - timedelta(days=days)
        try:
            resp = requests.get(
                BARS_URL.format(symbol=symbol),
                headers={
                    "APCA-API-KEY-ID":     config.ALPACA_API_KEY,
                    "APCA-API-SECRET-KEY": config.ALPACA_SECRET_KEY,
                },
                params={
                    "timeframe": "1Day",
                    "start":     start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "end":       end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "limit":     50,
                    "feed":      "iex",
                },
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json().get("bars") or []
        except Exception as e:
            log.error(f"[PROFILER] Failed to fetch bars for {symbol}: {e}")
            return []

    def _fetch_intraday_bars(self, symbol: str) -> list:
        today = date.today()
        start = datetime.combine(today, datetime.min.time()).replace(hour=13, minute=30)
        end   = datetime.combine(today, datetime.min.time()).replace(hour=20, minute=0)
        try:
            resp = requests.get(
                BARS_URL.format(symbol=symbol),
                headers={
                    "APCA-API-KEY-ID":     config.ALPACA_API_KEY,
                    "APCA-API-SECRET-KEY": config.ALPACA_SECRET_KEY,
                },
                params={
                    "timeframe": "5Min",
                    "start":     start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "end":       end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "limit":     100,
                    "feed":      "iex",
                },
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json().get("bars") or []
        except Exception as e:
            log.warning(f"[PROFILER] Could not fetch intraday bars for {symbol}: {e}")
            return []

    def _fetch_trade_history(self, symbol: str) -> dict:
        cutoff = datetime.utcnow() - timedelta(days=30)
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status != 'open')              as total,
                    COUNT(*) FILTER (WHERE pnl_usd > 0)                   as wins,
                    COUNT(*) FILTER (WHERE exit_reason = 'stop')          as stop_outs,
                    COUNT(*) FILTER (WHERE exit_reason = 'take_profit')   as take_profits,
                    COALESCE(AVG(pnl_usd) FILTER (WHERE pnl_usd > 0), 0) as avg_win,
                    COALESCE(AVG(pnl_usd) FILTER (WHERE pnl_usd <= 0 AND status != 'open'), 0) as avg_loss,
                    COALESCE(SUM(pnl_usd) FILTER (WHERE status != 'open'), 0) as total_pnl,
                    COALESCE(AVG(
                        EXTRACT(EPOCH FROM (exited_at - entered_at)) / 60
                    ) FILTER (WHERE status != 'open'), 0) as avg_hold_minutes
                FROM trades
                WHERE symbol = %s AND entered_at >= %s
            """, (symbol, cutoff))
            stats = dict(cur.fetchone())
            cur.execute("""
                SELECT
                    side,
                    ROUND(entry_price::numeric, 2) as entry,
                    ROUND(exit_price::numeric, 2)  as exit,
                    ROUND(stop_loss::numeric, 2)   as stop,
                    ROUND(take_profit::numeric, 2) as tp,
                    ROUND(pnl_usd::numeric, 2)     as pnl,
                    exit_reason,
                    signal_score,
                    EXTRACT(EPOCH FROM (exited_at - entered_at)) / 60 as hold_mins
                FROM trades
                WHERE symbol = %s AND entered_at >= %s AND status != 'open'
                ORDER BY entered_at DESC
                LIMIT 20
            """, (symbol, cutoff))
            recent_trades = [dict(r) for r in cur.fetchall()]
        total = stats["total"] or 0
        wins  = stats["wins"]  or 0
        stats["win_rate"]      = round(wins / total * 100, 1) if total > 0 else 0
        stats["recent_trades"] = recent_trades
        return stats

    def _compute_bar_stats(self, symbol: str, daily_bars: list, intraday_bars: list) -> dict:
        if not daily_bars:
            return {}
        true_ranges = []
        closes = []
        for i, bar in enumerate(daily_bars):
            high  = bar.get("h", 0)
            low   = bar.get("l", 0)
            close = bar.get("c", 0)
            prev_close = daily_bars[i-1].get("c", close) if i > 0 else close
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(tr)
            closes.append(close)
        current_price   = closes[-1] if closes else 0
        avg_daily_atr   = sum(true_ranges[-14:]) / min(len(true_ranges), 14) if true_ranges else 0
        avg_daily_range = sum(bar.get("h",0) - bar.get("l",0) for bar in daily_bars) / len(daily_bars)
        intraday_trs = []
        if intraday_bars:
            for i, bar in enumerate(intraday_bars):
                h = bar.get("h", 0)
                l = bar.get("l", 0)
                prev_c = intraday_bars[i-1].get("c", bar.get("c", 0)) if i > 0 else bar.get("c", 0)
                tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
                intraday_trs.append(tr)
        avg_5min_atr     = sum(intraday_trs[-14:]) / min(len(intraday_trs), 14) if intraday_trs else 0
        atr_pct_of_price = (avg_daily_atr / current_price * 100) if current_price > 0 else 0
        intraday_atr_pct = (avg_5min_atr  / current_price * 100) if current_price > 0 else 0
        return {
            "current_price":       round(current_price, 2),
            "avg_daily_atr":       round(avg_daily_atr, 4),
            "avg_daily_range":     round(avg_daily_range, 4),
            "avg_5min_atr":        round(avg_5min_atr, 4),
            "atr_pct_of_price":    round(atr_pct_of_price, 3),
            "intraday_atr_pct":    round(intraday_atr_pct, 3),
            "days_of_data":        len(daily_bars),
            "intraday_bars_today": len(intraday_bars),
        }

    def _profile_symbol(self, symbol: str):
        log.info(f"[PROFILER] Analyzing {symbol}...")

        daily_bars    = self._fetch_bars(symbol, days=30)
        intraday_bars = self._fetch_intraday_bars(symbol)
        trade_history = self._fetch_trade_history(symbol)
        bar_stats     = self._compute_bar_stats(symbol, daily_bars, intraday_bars)

        if not bar_stats:
            log.warning(f"[PROFILER] No bar data for {symbol} — skipping")
            return

        is_leveraged = symbol.upper() in LEVERAGED_ETFS
        leveraged_note = (
            f"\nIMPORTANT: {symbol} is a 3x LEVERAGED ETF. "
            f"Set min_atr_pct to at least 0.010 (1.0%) — never below this. "
            f"Stops must be wide enough to survive normal intraday swings without being whipsawed."
        ) if is_leveraged else ""

        prompt = f"""You are configuring a day trading bot for the stock {symbol}.

The bot uses these parameters per symbol:
- atr_stop_mult: how many ATRs below entry to place stop loss (currently global: {config.ATR_STOP_MULT})
- atr_tp_mult: how many ATRs above entry to place take profit (currently global: {config.ATR_TP_MULT})
- breakeven_mult: how many ATRs of profit before moving stop to entry (currently global: {config.BREAKEVEN_ATR_MULT})
- volume_spike_mult: how many times above average volume counts as a spike (currently global: {config.VOL_ACCEL_MULT})
- min_atr_floor: minimum ATR in dollars regardless of what the indicator says
- min_atr_pct: minimum ATR as a percentage of price (0.003 = 0.3%){leveraged_note}

Here is the REAL data for {symbol}:

VOLATILITY STATS (from last 30 days of actual price data):
{json.dumps(bar_stats, indent=2)}

OUR TRADE HISTORY on {symbol} (last 30 days):
{json.dumps(trade_history, indent=2, default=float)}

CURRENT GLOBAL CONFIG:
- ATR_STOP_MULT: {config.ATR_STOP_MULT}
- ATR_TP_MULT: {config.ATR_TP_MULT}
- BREAKEVEN_ATR_MULT: {config.BREAKEVEN_ATR_MULT}
- VOL_ACCEL_MULT: {config.VOL_ACCEL_MULT}

Based on this real data, generate optimal per-symbol config values for {symbol}.

Important things to consider:
- If intraday_atr_pct is very small (under 0.3%), set min_atr_pct to at least 0.3% to prevent stops from being placed above entry
- If stop_outs are high, consider widening the stop (higher atr_stop_mult)
- If take_profits are rarely hit, consider tightening tp (lower atr_tp_mult)
- If avg_hold_minutes is very short before stops hit, stops are too tight
- The min_atr_floor in dollars should be price * min_atr_pct

Respond ONLY with a valid JSON object, no explanation, no markdown, no backticks:
{{
  "atr_stop_mult": <float>,
  "atr_tp_mult": <float>,
  "breakeven_mult": <float>,
  "volume_spike_mult": <float>,
  "min_atr_floor": <float>,
  "min_atr_pct": <float>,
  "notes": "<one sentence explaining the key reason for these values>"
}}"""

        try:
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":         config.ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      "claude-haiku-4-5-20251001",
                    "max_tokens": 300,
                    "messages":   [{"role": "user", "content": prompt}]
                },
                timeout=30,
            )
            response.raise_for_status()
            raw = response.json()["content"][0]["text"].strip()

            if raw.startswith('```'):
                lines = raw.split('\n')
                lines = lines[1:]
                if lines and lines[-1].strip() == '```':
                    lines = lines[:-1]
                raw = '\n'.join(lines).strip()

            profile = json.loads(raw)

            required = ["atr_stop_mult", "atr_tp_mult", "breakeven_mult",
                        "volume_spike_mult", "min_atr_floor", "min_atr_pct", "notes"]
            for key in required:
                if key not in profile:
                    raise ValueError(f"Missing key: {key}")

            self._save_profile(symbol, profile, raw, bar_stats.get("current_price", 0))

            log.info(
                f"[PROFILER] {symbol} profile saved | "
                f"stop={profile['atr_stop_mult']}x "
                f"tp={profile['atr_tp_mult']}x "
                f"floor=${profile['min_atr_floor']:.3f} "
                f"({profile['min_atr_pct']*100:.2f}% of price)"
            )
            log.info(f"[PROFILER] {symbol} notes: {profile['notes']}")

        except json.JSONDecodeError as e:
            log.error(f"[PROFILER] {symbol}: Claude returned invalid JSON — {e}")
            log.error(f"[PROFILER] Raw response: {raw[:200]}")
        except Exception as e:
            log.error(f"[PROFILER] {symbol}: Claude API failed — {e}")

    def _save_profile(self, symbol: str, profile: dict, raw: str, current_price: float = 0):
        # ── Enforce minimum R:R ───────────────────────────────
        min_rr = getattr(config, "MIN_RR", 1.0)
        actual_rr = profile["atr_tp_mult"] / profile["atr_stop_mult"]
        if actual_rr < min_rr:
            profile["atr_tp_mult"] = round(profile["atr_stop_mult"] * min_rr * 1.1, 2)
            log.warning(
                f"[PROFILER] {symbol} R:R too low ({actual_rr:.2f}) — "
                f"bumping atr_tp_mult to {profile['atr_tp_mult']}x"
            )

        # ── Enforce ATR floors for leveraged ETFs ─────────────
        # Claude sometimes sets floors too tight for 3x ETFs.
        # This is a hard override — never rely on Claude to get this right.
        sym_upper = symbol.upper()
        if sym_upper in LEVERAGED_ETFS:
            if profile["min_atr_pct"] < LEVERAGED_ETF_MIN_ATR_PCT:
                log.warning(
                    f"[PROFILER] {symbol} is a leveraged ETF — "
                    f"overriding min_atr_pct {profile['min_atr_pct']:.4f} → {LEVERAGED_ETF_MIN_ATR_PCT:.4f}"
                )
                profile["min_atr_pct"] = LEVERAGED_ETF_MIN_ATR_PCT
            # Also enforce floor in dollars
            if current_price > 0:
                enforced_floor = current_price * LEVERAGED_ETF_MIN_ATR_PCT
                if profile["min_atr_floor"] < enforced_floor:
                    log.warning(
                        f"[PROFILER] {symbol} min_atr_floor ${profile['min_atr_floor']:.3f} "
                        f"→ ${enforced_floor:.3f} (1% of ${current_price:.2f})"
                    )
                    profile["min_atr_floor"] = round(enforced_floor, 3)
        else:
            # Normal symbols — still enforce the global minimum
            if profile["min_atr_pct"] < NORMAL_MIN_ATR_PCT:
                profile["min_atr_pct"] = NORMAL_MIN_ATR_PCT
                if current_price > 0:
                    profile["min_atr_floor"] = max(
                        profile["min_atr_floor"],
                        round(current_price * NORMAL_MIN_ATR_PCT, 3)
                    )

        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO symbol_profiles
                    (symbol, atr_stop_mult, atr_tp_mult, breakeven_mult,
                     volume_spike_mult, min_atr_floor, min_atr_pct,
                     notes, raw_response, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    atr_stop_mult     = EXCLUDED.atr_stop_mult,
                    atr_tp_mult       = EXCLUDED.atr_tp_mult,
                    breakeven_mult    = EXCLUDED.breakeven_mult,
                    volume_spike_mult = EXCLUDED.volume_spike_mult,
                    min_atr_floor     = EXCLUDED.min_atr_floor,
                    min_atr_pct       = EXCLUDED.min_atr_pct,
                    notes             = EXCLUDED.notes,
                    raw_response      = EXCLUDED.raw_response,
                    updated_at        = NOW()
            """, (
                symbol.upper(),
                float(profile["atr_stop_mult"]),
                float(profile["atr_tp_mult"]),
                float(profile["breakeven_mult"]),
                float(profile["volume_spike_mult"]),
                float(profile["min_atr_floor"]),
                float(profile["min_atr_pct"]),
                str(profile["notes"]),
                raw,
            ))
