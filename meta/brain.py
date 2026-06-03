"""
meta/brain.py — Meta Brain

Runs daily after market close. Analyzes the last 7 days of trades
and signals to find patterns. Adjusts strategy thresholds based on
what's actually working vs what's being missed.

Also fetches the full day's 1-minute bars for each symbol and sends
them to Claude so it can see the actual price action and identify
moves the bot missed, bad stop placement, etc.

Uses Claude API for the daily report — feeds it the raw stats AND
full bar data. Math still handles threshold adjustments so they're
deterministic. Claude explains what's happening and gives suggestions.

v2 fix: _adjust_thresholds now reads live DB values via get_config_override()
instead of hardcoded config.X defaults. Previously the meta brain was making
the same adjustment every single day (e.g. MIN_SIGNAL_SCORE: 3→4 forever)
because it always read from config.py (always 3) instead of the DB (already 4).
"""

import os
import base64
import json
import logging
import requests
import psycopg2.extras
from datetime import datetime, date, timedelta
from core.database import get_conn, set_config_override, get_config_override
from core.notifier import notify_eod_summary
import config

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = os.environ.get("GITHUB_REPO", "JacksonIsAwesome/futures_bot")
GITHUB_API   = "https://api.github.com"

ANTHROPIC_API_KEY = config.ANTHROPIC_API_KEY

log = logging.getLogger(__name__)

# Alpaca bars endpoint
BARS_URL = "https://data.alpaca.markets/v2/stocks/{symbol}/bars"


class MetaBrain:
    def __init__(self):
        log.info("[META] Meta brain initialized ✓")

    def run_review(self):
        """Full daily review. Call this after market close."""
        log.info("[META] ═══════════════════════════════════════════")
        log.info("[META] Starting daily review...")

        stats   = self._gather_stats()
        missed  = self._find_missed_opportunities()
        issues  = self._identify_issues(stats)
        adjusts = self._adjust_thresholds(stats, missed)
        bars    = self._fetch_daily_bars()
        report  = self._write_report(stats, missed, issues, adjusts, bars)

        self._save_review(stats, missed, issues, adjusts, report)

        # ── EOD SMS summary ───────────────────────────────────
        try:
            notify_eod_summary(
                trades=stats.get("total", 0),
                wins=stats.get("wins", 0),
                pnl=stats.get("total_pnl", 0)
            )
        except Exception as e:
            log.error(f"[META] EOD SMS failed: {e}")

        # ── Commit suggestions to GitHub ──────────────────────
        try:
            self._write_suggestions_to_github(report, stats, str(date.today()))
        except Exception as e:
            log.error(f"[META] GitHub suggestions failed: {e}")

        log.info("[META] Daily review complete ✓")
        log.info("[META] ═══════════════════════════════════════════")
        return report

    # ── Daily bar fetching ────────────────────────────────────────

    def _fetch_daily_bars(self) -> dict:
        """
        Fetch today's full 1-minute bars for each symbol from Alpaca.
        Returns dict of symbol -> list of simplified bar dicts.
        Falls back to empty dict if API fails.
        """
        result = {}
        today  = date.today()

        # market open/close in UTC (ET + 4h)
        start = datetime.combine(today, datetime.min.time()).replace(hour=13, minute=30)
        end   = datetime.combine(today, datetime.min.time()).replace(hour=20, minute=0)

        for symbol in config.SYMBOLS:
            try:
                resp = requests.get(
                    BARS_URL.format(symbol=symbol),
                    headers={
                        "APCA-API-KEY-ID":     config.ALPACA_API_KEY,
                        "APCA-API-SECRET-KEY": config.ALPACA_SECRET_KEY,
                    },
                    params={
                        "timeframe": "1Min",
                        "start":     start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "end":       end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "limit":     400,   # full trading day = ~390 1-min bars
                        "feed":      "iex",
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                raw_bars = resp.json().get("bars") or []

                simplified = []
                for b in raw_bars:
                    t = b.get("t", "")
                    try:
                        dt      = datetime.fromisoformat(t.replace("Z", "+00:00"))
                        et_hour = (dt.hour - 4) % 24
                        time_str = f"{et_hour:02d}:{dt.minute:02d}"
                    except Exception:
                        time_str = t

                    simplified.append({
                        "time_et": time_str,
                        "o": round(b.get("o", 0), 2),
                        "h": round(b.get("h", 0), 2),
                        "l": round(b.get("l", 0), 2),
                        "c": round(b.get("c", 0), 2),
                        "v": b.get("v", 0),
                    })

                result[symbol] = simplified
                log.info(f"[META] Fetched {len(simplified)} bars for {symbol}")

            except Exception as e:
                log.error(f"[META] Failed to fetch bars for {symbol}: {e}")
                result[symbol] = []

        return result

    # ── Data gathering ────────────────────────────────────────────

    def _gather_stats(self) -> dict:
        """Pull last 7 days of trade performance."""
        cutoff = datetime.utcnow() - timedelta(days=config.META_LOOKBACK_DAYS)

        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status != 'open') as total,
                    COUNT(*) FILTER (WHERE pnl_usd > 0)      as wins,
                    COUNT(*) FILTER (WHERE pnl_usd <= 0 AND status != 'open') as losses,
                    COALESCE(AVG(pnl_usd) FILTER (WHERE pnl_usd > 0), 0)                     as avg_win,
                    COALESCE(AVG(pnl_usd) FILTER (WHERE pnl_usd <= 0 AND status != 'open'), 0) as avg_loss,
                    COALESCE(SUM(pnl_usd) FILTER (WHERE status != 'open'), 0) as total_pnl,
                    COALESCE(AVG(signal_score), 0) as avg_score
                FROM trades
                WHERE entered_at >= %s
            """, (cutoff,))
            row = dict(cur.fetchone())

            total = row["total"] or 0
            wins  = row["wins"]  or 0
            row["win_rate"] = (wins / total * 100) if total > 0 else 0

            avg_win  = abs(row["avg_win"])
            avg_loss = abs(row["avg_loss"])
            row["avg_rr"] = (avg_win / avg_loss) if avg_loss > 0 else 0

            cur.execute("""
                SELECT
                    EXTRACT(HOUR FROM entered_at AT TIME ZONE 'America/New_York') as hour,
                    COUNT(*) FILTER (WHERE pnl_usd > 0)                           as wins,
                    COUNT(*) FILTER (WHERE pnl_usd <= 0 AND status != 'open')     as losses,
                    COALESCE(SUM(pnl_usd), 0)                                     as pnl
                FROM trades
                WHERE entered_at >= %s AND status != 'open'
                GROUP BY hour
                ORDER BY pnl DESC
            """, (cutoff,))
            row["by_hour"] = [dict(r) for r in cur.fetchall()]

            cur.execute("""
                SELECT
                    symbol,
                    COUNT(*) FILTER (WHERE pnl_usd > 0)                       as wins,
                    COUNT(*) FILTER (WHERE pnl_usd <= 0 AND status != 'open') as losses,
                    COALESCE(SUM(pnl_usd), 0)                                 as pnl
                FROM trades
                WHERE entered_at >= %s AND status != 'open'
                GROUP BY symbol
                ORDER BY pnl DESC
            """, (cutoff,))
            row["by_symbol"] = [dict(r) for r in cur.fetchall()]

            cur.execute("""
                SELECT exit_reason, COUNT(*) as count,
                       COALESCE(SUM(pnl_usd), 0) as pnl
                FROM trades
                WHERE entered_at >= %s AND status != 'open'
                GROUP BY exit_reason
            """, (cutoff,))
            row["by_exit"] = [dict(r) for r in cur.fetchall()]

            today_cutoff = datetime.utcnow().replace(hour=0, minute=0, second=0)
            cur.execute("""
                SELECT
                    symbol, side, signal_score,
                    entry_price, exit_price, pnl_usd,
                    stop_loss, take_profit, exit_reason,
                    entered_at AT TIME ZONE 'America/New_York' as entry_et,
                    exited_at  AT TIME ZONE 'America/New_York' as exit_et
                FROM trades
                WHERE entered_at >= %s AND status != 'open'
                ORDER BY entered_at
            """, (today_cutoff,))
            row["todays_trades"] = [dict(r) for r in cur.fetchall()]

        return row

    def _find_missed_opportunities(self) -> dict:
        cutoff = datetime.utcnow() - timedelta(days=config.META_LOOKBACK_DAYS)

        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            cur.execute("""
                SELECT * FROM signals
                WHERE timestamp >= %s
                  AND traded = FALSE
                  AND direction IS NOT NULL
                ORDER BY score DESC
            """, (cutoff,))
            untraded     = cur.fetchall()
            missed_total = len(untraded)

            cur.execute("""
                SELECT COUNT(*) FROM signals
                WHERE timestamp >= %s
                  AND traded = FALSE
                  AND would_have_won = TRUE
            """, (cutoff,))
            missed_wins = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM signals
                WHERE timestamp >= %s
                  AND traded = FALSE
                  AND score = %s
            """, (cutoff, config.MIN_SIGNAL_SCORE - 1))
            near_misses = cur.fetchone()[0]

        return {
            "total_untraded": missed_total,
            "missed_wins":    missed_wins,
            "near_misses":    near_misses,
            "miss_rate":      (missed_wins / missed_total * 100) if missed_total > 0 else 0
        }

    def _identify_issues(self, stats: dict) -> str:
        win_rate = stats.get("win_rate", 0)
        avg_rr   = stats.get("avg_rr",   0)
        total    = stats.get("total",     0)

        if total < config.META_MIN_TRADES:
            return f"Not enough trades yet ({total}/{config.META_MIN_TRADES} needed for analysis)"
        if win_rate < 40:
            return f"Win rate too low ({win_rate:.1f}%) — signals firing on weak setups"
        if avg_rr < 1.5:
            return f"R:R ratio too low ({avg_rr:.2f}) — take profits too tight or stops too wide"

        by_hour = stats.get("by_hour", [])
        if by_hour:
            worst = min(by_hour, key=lambda x: x["pnl"])
            if worst["pnl"] < -50:
                return f"Hour {int(worst['hour'])}:00 ET is consistently losing (${worst['pnl']:.0f})"

        by_symbol = stats.get("by_symbol", [])
        if by_symbol:
            worst_sym = min(by_symbol, key=lambda x: x["pnl"])
            if worst_sym["pnl"] < -100:
                return f"{worst_sym['symbol']} is losing money (${worst_sym['pnl']:.0f}) — consider removing"

        return "No major issues identified — keep running"

    def _identify_top_win(self, stats: dict) -> str:
        by_symbol = stats.get("by_symbol", [])
        if by_symbol:
            best = max(by_symbol, key=lambda x: x["pnl"])
            if best["pnl"] > 50:
                wr = best["wins"] / (best["wins"] + best["losses"]) * 100 if (best["wins"] + best["losses"]) > 0 else 0
                return f"{best['symbol']} is the best performer (${best['pnl']:.0f}, {wr:.0f}% win rate)"

        by_hour = stats.get("by_hour", [])
        if by_hour:
            best_hour = max(by_hour, key=lambda x: x["pnl"])
            if best_hour["pnl"] > 50:
                return f"Hour {int(best_hour['hour'])}:00 ET is the best trading hour (${best_hour['pnl']:.0f})"

        return "No standout winners yet — need more data"

    def _adjust_thresholds(self, stats: dict, missed: dict) -> dict:
        """
        Adjust config overrides based on 7-day performance.

        IMPORTANT: reads live DB values via get_config_override() so each
        adjustment builds on the previous one. The old version read from
        config.X (Python file defaults) which caused the same adjustment
        to repeat every day (e.g. MIN_SIGNAL_SCORE: 3→4 forever because
        config.MIN_SIGNAL_SCORE is always 3, ignoring the DB value of 4).
        """
        adjustments = {}
        total = stats.get("total", 0)

        if total < config.META_MIN_TRADES:
            log.info(f"[META] Not enough trades for threshold adjustment ({total}/{config.META_MIN_TRADES})")
            return adjustments

        step     = config.META_ADJUST_STEP
        win_rate = stats.get("win_rate", 0)
        avg_rr   = stats.get("avg_rr",   0)
        missed_w = missed.get("missed_wins",  0)
        near_m   = missed.get("near_misses",  0)

        # ── MIN_SIGNAL_SCORE ──────────────────────────────────────────────────
        # Read from DB so increments accumulate (3→4→5) not loop (3→4→3→4).
        if win_rate < 45:
            current = int(get_config_override("MIN_SIGNAL_SCORE", config.MIN_SIGNAL_SCORE))
            new_val = min(current + 1, 5)
            if new_val != current:
                set_config_override("MIN_SIGNAL_SCORE", new_val)
                adjustments["MIN_SIGNAL_SCORE"] = f"{current} → {new_val} (win rate too low)"
            else:
                log.info(f"[META] MIN_SIGNAL_SCORE already at max ({current}) — no change")

        elif win_rate > 65 and missed_w > 5:
            current = int(get_config_override("MIN_SIGNAL_SCORE", config.MIN_SIGNAL_SCORE))
            new_val = max(current - 1, 2)
            if new_val != current:
                set_config_override("MIN_SIGNAL_SCORE", new_val)
                adjustments["MIN_SIGNAL_SCORE"] = f"{current} → {new_val} (missing too many wins)"

        # ── ATR_TP_MULT ───────────────────────────────────────────────────────
        # Read from DB so 4.0→4.1→4.2 accumulates instead of looping at 4.1.
        if avg_rr < 1.5 and total >= config.META_MIN_TRADES:
            current = float(get_config_override("ATR_TP_MULT", config.ATR_TP_MULT))
            new_val = round(current + step, 2)
            set_config_override("ATR_TP_MULT", new_val)
            adjustments["ATR_TP_MULT"] = f"{current} → {new_val} (R:R too low)"

        elif avg_rr > 3.0:
            current = float(get_config_override("ATR_STOP_MULT", config.ATR_STOP_MULT))
            new_val = round(current - step, 2)
            if new_val >= 0.5:
                set_config_override("ATR_STOP_MULT", new_val)
                adjustments["ATR_STOP_MULT"] = f"{current} → {new_val} (R:R excellent, tightening stops)"

        # ── VOLUME_SPIKE_MULT ─────────────────────────────────────────────────
        # Read from DB so 1.5→1.4→1.3 accumulates instead of looping at 1.4.
        if near_m > 10:
            current = float(get_config_override("VOLUME_SPIKE_MULT", config.VOLUME_SPIKE_MULT))
            new_val = round(current - step, 2)
            if new_val >= 1.0:
                set_config_override("VOLUME_SPIKE_MULT", new_val)
                adjustments["VOLUME_SPIKE_MULT"] = f"{current} → {new_val} (too many near misses)"

        return adjustments

    def _write_report(self, stats, missed, issues, adjustments, bars) -> str:
        """
        Call Claude API with raw stats + full day bar data.
        Falls back to simple report if API fails.
        """
        top_win = self._identify_top_win(stats)

        todays_trades = []
        for t in stats.get("todays_trades", []):
            todays_trades.append({
                "symbol":      t["symbol"],
                "side":        t["side"],
                "score":       t["signal_score"],
                "entry_price": round(float(t["entry_price"] or 0), 2),
                "exit_price":  round(float(t["exit_price"]  or 0), 2),
                "pnl":         round(float(t["pnl_usd"]     or 0), 2),
                "stop_loss":   round(float(t["stop_loss"]   or 0), 2),
                "take_profit": round(float(t["take_profit"] or 0), 2),
                "exit_reason": t["exit_reason"],
                "entry_time":  str(t["entry_et"])[:16] if t["entry_et"] else None,
                "exit_time":   str(t["exit_et"])[:16]  if t["exit_et"]  else None,
            })

        bar_data = {}
        for symbol, symbol_bars in bars.items():
            if symbol_bars:
                bar_data[symbol] = symbol_bars
                closes  = [b["c"] for b in symbol_bars]
                volumes = [b["v"] for b in symbol_bars]
                bar_data[f"{symbol}_summary"] = {
                    "open":           symbol_bars[0]["o"] if symbol_bars else None,
                    "close":          symbol_bars[-1]["c"] if symbol_bars else None,
                    "day_high":       max(b["h"] for b in symbol_bars) if symbol_bars else None,
                    "day_low":        min(b["l"] for b in symbol_bars) if symbol_bars else None,
                    "total_bars":     len(symbol_bars),
                    "avg_volume":     round(sum(volumes) / len(volumes)) if volumes else 0,
                    "max_volume_bar": max(symbol_bars, key=lambda b: b["v"]) if symbol_bars else None,
                }

        data_summary = {
            "date":          str(date.today()),
            "lookback_days": config.META_LOOKBACK_DAYS,
            "performance_7d": {
                "total_trades":    stats.get("total",     0),
                "win_rate_pct":    round(stats.get("win_rate", 0), 1),
                "avg_rr":          round(stats.get("avg_rr",   0), 2),
                "total_pnl_usd":   round(stats.get("total_pnl", 0), 2),
                "avg_signal_score":round(stats.get("avg_score",  0), 1),
            },
            "todays_trades": todays_trades,
            "missed_opportunities": {
                "untraded_signals": missed.get("total_untraded", 0),
                "would_have_won":   missed.get("missed_wins",    0),
                "near_misses":      missed.get("near_misses",    0),
            },
            "by_hour": [
                {
                    "hour_et": int(h["hour"]),
                    "wins":    h["wins"],
                    "losses":  h["losses"],
                    "pnl":     round(h["pnl"], 2)
                }
                for h in sorted(stats.get("by_hour", []), key=lambda x: x["hour"])
            ],
            "by_symbol": [
                {
                    "symbol": s["symbol"],
                    "wins":   s["wins"],
                    "losses": s["losses"],
                    "pnl":    round(s["pnl"], 2)
                }
                for s in sorted(stats.get("by_symbol", []),
                                key=lambda x: x["pnl"], reverse=True)
            ],
            "auto_adjustments_made": adjustments,
            "top_issue": issues,
            "top_win":   top_win,
            "current_config": {
                # ── Entry gates ───────────────────────────────
                "MIN_SIGNAL_SCORE":            get_config_override("MIN_SIGNAL_SCORE",            config.MIN_SIGNAL_SCORE),
                "PRIME_BASE_MIN":              get_config_override("PRIME_BASE_MIN",              config.PRIME_BASE_MIN),
                "REGULAR_BASE_MIN":            get_config_override("REGULAR_BASE_MIN",            config.REGULAR_BASE_MIN),
                "PRIME_END_HOUR":              get_config_override("PRIME_END_HOUR",              config.PRIME_END_HOUR),
                # ── Momentum gate ─────────────────────────────
                "MOMENTUM_GATE_ENABLED":       get_config_override("MOMENTUM_GATE_ENABLED",       config.MOMENTUM_GATE_ENABLED),
                "MOMENTUM_GATE_MIN":           get_config_override("MOMENTUM_GATE_MIN",           config.MOMENTUM_GATE_MIN),
                "ROC_PERIOD":                  get_config_override("ROC_PERIOD",                  config.ROC_PERIOD),
                "ROC_MIN_LONG":                get_config_override("ROC_MIN_LONG",                config.ROC_MIN_LONG),
                "ROC_MIN_SHORT":               get_config_override("ROC_MIN_SHORT",               config.ROC_MIN_SHORT),
                "MACD_FAST":                   get_config_override("MACD_FAST",                   config.MACD_FAST),
                "MACD_SLOW":                   get_config_override("MACD_SLOW",                   config.MACD_SLOW),
                "MACD_SIGNAL_PERIOD":          get_config_override("MACD_SIGNAL_PERIOD",          config.MACD_SIGNAL_PERIOD),
                "CANDLE_CONSISTENCY_LOOKBACK": get_config_override("CANDLE_CONSISTENCY_LOOKBACK", config.CANDLE_CONSISTENCY_LOOKBACK),
                "CANDLE_CONSISTENCY_MIN":      get_config_override("CANDLE_CONSISTENCY_MIN",      config.CANDLE_CONSISTENCY_MIN),
                # ── MTF / VWAP / Volume ───────────────────────
                "MTF_FILTER_ENABLED":          get_config_override("MTF_FILTER_ENABLED",          config.MTF_FILTER_ENABLED),
                "MTF_EMA_PERIOD":              get_config_override("MTF_EMA_PERIOD",              config.MTF_EMA_PERIOD),
                "VWAP_DEV_MULT":               get_config_override("VWAP_DEV_MULT",               config.VWAP_DEV_MULT),
                "VOL_ACCEL_MULT":              get_config_override("VOL_ACCEL_MULT",              config.VOL_ACCEL_MULT),
                "VOLUME_SPIKE_MULT":           get_config_override("VOLUME_SPIKE_MULT",           config.VOLUME_SPIKE_MULT),
                # ── RSI ───────────────────────────────────────
                "RSI_OVERBOUGHT":              get_config_override("RSI_OVERBOUGHT",              config.RSI_OVERBOUGHT),
                "RSI_OVERSOLD":                get_config_override("RSI_OVERSOLD",                config.RSI_OVERSOLD),
                # ── Stops / TP ────────────────────────────────
                "ATR_STOP_MULT":               get_config_override("ATR_STOP_MULT",               config.ATR_STOP_MULT),
                "ATR_TP_MULT":                 get_config_override("ATR_TP_MULT",                 config.ATR_TP_MULT),
                "BREAKEVEN_ATR_MULT":          get_config_override("BREAKEVEN_ATR_MULT",          config.BREAKEVEN_ATR_MULT),
                "MIN_RR":                      get_config_override("MIN_RR",                      config.MIN_RR),
                # ── Risk ──────────────────────────────────────
                "SIMULATED_LEVERAGE":          get_config_override("SIMULATED_LEVERAGE",          config.SIMULATED_LEVERAGE),
                "MAX_DAILY_LOSS_PCT":          get_config_override("MAX_DAILY_LOSS_PCT",          config.MAX_DAILY_LOSS_PCT),
                "MAX_OPEN_TRADES":             get_config_override("MAX_OPEN_TRADES",             config.MAX_OPEN_TRADES),
                "MAX_POSITION_PCT":            get_config_override("MAX_POSITION_PCT",            config.MAX_POSITION_PCT),
                "LOSS_COOLDOWN_MINS":          get_config_override("LOSS_COOLDOWN_MINS",          config.LOSS_COOLDOWN_MINS),
                # ── Direction flip ────────────────────────────
                "FLIP_ENABLED":                get_config_override("FLIP_ENABLED",                config.FLIP_ENABLED),
                "FLIP_MIN_SIGNALS":            get_config_override("FLIP_MIN_SIGNALS",            config.FLIP_MIN_SIGNALS),
                "FLIP_BASE_SCORE_MIN":         get_config_override("FLIP_BASE_SCORE_MIN",         config.FLIP_BASE_SCORE_MIN),
                # ── Dynamic TP ────────────────────────────────
                "DYNAMIC_TP_ENABLED":          get_config_override("DYNAMIC_TP_ENABLED",          config.DYNAMIC_TP_ENABLED),
                "DYNAMIC_TP_EXTENSION":        get_config_override("DYNAMIC_TP_EXTENSION",        config.DYNAMIC_TP_EXTENSION),
                "DYNAMIC_TP_MIN_MOMENTUM":     get_config_override("DYNAMIC_TP_MIN_MOMENTUM",     config.DYNAMIC_TP_MIN_MOMENTUM),
                # ── Scan speed ────────────────────────────────
                "FAST_SCAN_ENABLED":           get_config_override("FAST_SCAN_ENABLED",           config.FAST_SCAN_ENABLED),
                "FAST_SCAN_SCORE":             get_config_override("FAST_SCAN_SCORE",             config.FAST_SCAN_SCORE),
                "FAST_SCAN_INTERVAL":          get_config_override("FAST_SCAN_INTERVAL",          config.FAST_SCAN_INTERVAL),
            },
            "todays_price_action": bar_data,
        }

        prompt = f"""You are the meta brain of AlphaBot, an EMA/VWAP momentum day trading bot.

Here is today's full data including the complete 1-minute bar history for each symbol:

{json.dumps(data_summary, indent=2, default=float)}

CONFIG REFERENCE (current_config shows live values including any DB overrides):
- MIN_SIGNAL_SCORE: base signals needed out of 5 to consider a trade
- PRIME_BASE_MIN / REGULAR_BASE_MIN: score thresholds during prime (9:30-11am) vs rest of day
- MOMENTUM_GATE_ENABLED/MIN: require ROC+MACD+CandleConsistency confirmation (0-3 score)
- ROC_PERIOD/MIN_LONG/MIN_SHORT: price acceleration % needed over N candles
- MACD_FAST/SLOW/SIGNAL_PERIOD: MACD histogram must be growing in signal direction
- CANDLE_CONSISTENCY_LOOKBACK/MIN: N of last M candles must close in signal direction
- MTF_FILTER_ENABLED/PERIOD: slow EMA on 1-min candles must agree with direction
- VWAP_DEV_MULT: price must be X std devs from VWAP (breakout strength)
- VOL_ACCEL_MULT: projected candle volume must be X× the 20-candle average
- RSI_OVERBOUGHT/OVERSOLD: RSI gates — blocks longs near OB, shorts near OS
- ATR_STOP_MULT: stop = entry ± (ATR × this). Wider = more room, more risk
- ATR_TP_MULT: take profit = entry ± (ATR × this). Higher = bigger targets
- BREAKEVEN_ATR_MULT: move stop to entry once price moves (ATR × this) in our favor
- MIN_RR: minimum reward:risk ratio required to enter
- SIMULATED_LEVERAGE: P&L multiplier (10x = $100 move shows as $1000)
- MAX_DAILY_LOSS_PCT: kill switch — shuts bot down if daily loss exceeds this %
- MAX_OPEN_TRADES: max simultaneous positions across all symbols
- MAX_POSITION_PCT: max % of capital in one trade
- LOSS_COOLDOWN_MINS: after a stop loss, block same symbol+direction for X minutes
- FLIP_ENABLED: exit trade if signals reverse direction
- FLIP_MIN_SIGNALS: # of confirming signals needed before re-entering after a flip
- FLIP_BASE_SCORE_MIN: minimum base score the flip signal must have to trigger
- DYNAMIC_TP_ENABLED: extend TP further if momentum still confirmed at first target
- DYNAMIC_TP_EXTENSION: extra ATR added to TP when momentum extends it
- FAST_SCAN_ENABLED: scan faster when a strong signal is detected
- FAST_SCAN_SCORE/INTERVAL: score threshold and interval (seconds) for fast scan

The bar data shows every 1-minute candle from today's session (time_et = Eastern Time).
Use it to understand what actually happened in the market today and compare it to
where the bot actually traded (see todays_trades).

Write a daily review. Keep it simple and direct. Include:
1. One sentence on overall performance
2. What the market actually did today (trending, choppy, big moves?)
3. Did the bot enter at good spots or bad spots based on the bar data?
4. What was the biggest move the bot missed and why?
5. One specific thing the bot is doing wrong
6. One or two concrete config changes to make tomorrow — reference exact variable
names from current_config and suggest specific values (e.g. "raise BREAKEVEN_ATR_MULT
from 1.5 to 2.0 because trades are getting shaken out too early")

Keep the whole thing under 300 words. Write it like you're talking to a high school
student who built this bot. Be specific — reference actual prices, times, and config
variable names."""

        try:
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":         ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json"
                },
                json={
                    "model":      "claude-sonnet-4-20250514",
                    "max_tokens": 1024,
                    "messages":   [{"role": "user", "content": prompt}]
                },
                timeout=45,
            )
            response.raise_for_status()
            claude_text = response.json()["content"][0]["text"]

        except Exception as e:
            log.error(f"[META] Claude API failed: {e} — using fallback report")
            claude_text = (
                f"API unavailable. Raw stats: "
                f"win_rate={stats.get('win_rate',0):.1f}% | "
                f"R:R={stats.get('avg_rr',0):.2f} | "
                f"P&L=${stats.get('total_pnl',0):.2f} | "
                f"issue={issues}"
            )

        header_lines = [
            f"═══════════ META BRAIN DAILY REPORT — {date.today()} ═══════════",
            f"Trades={stats.get('total',0)} | "
            f"Win={stats.get('win_rate',0):.1f}% | "
            f"R:R={stats.get('avg_rr',0):.2f} | "
            f"P&L=${stats.get('total_pnl',0):.2f}",
            f"",
            f"CLAUDE ANALYSIS:",
            claude_text,
            f"",
            f"AUTO-ADJUSTMENTS:",
        ]

        if adjustments:
            for k, v in adjustments.items():
                header_lines.append(f"  {k}: {v}")
        else:
            header_lines.append("  None")

        header_lines.append("═══════════════════════════════════════════════════════")
        report = "\n".join(header_lines)
        log.info("\n" + report)
        return report

    # ── GitHub integration ───────────────────────────────────

    def _fetch_github_file(self, path: str) -> str:
        """Fetch a file from the GitHub repo. Returns content or empty string."""
        if not GITHUB_TOKEN:
            return ""
        try:
            r = requests.get(
                f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{path}",
                headers={"Authorization": f"token {GITHUB_TOKEN}",
                         "Accept": "application/vnd.github.v3+json"},
                timeout=10
            )
            if r.status_code == 200:
                encoded = r.json().get("content", "")
                return base64.b64decode(encoded).decode("utf-8")
            log.warning(f"[META] GitHub fetch {path}: {r.status_code}")
            return ""
        except Exception as e:
            log.error(f"[META] GitHub fetch failed {path}: {e}")
            return ""

    def _commit_github_file(self, path: str, content: str, message: str):
        """Commit a file to the GitHub repo."""
        if not GITHUB_TOKEN:
            log.debug("[META] No GITHUB_TOKEN — skipping commit")
            return
        try:
            sha = None
            r = requests.get(
                f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{path}",
                headers={"Authorization": f"token {GITHUB_TOKEN}",
                         "Accept": "application/vnd.github.v3+json"},
                timeout=10
            )
            if r.status_code == 200:
                sha = r.json().get("sha")

            payload = {
                "message": message,
                "content": base64.b64encode(content.encode()).decode(),
            }
            if sha:
                payload["sha"] = sha

            r = requests.put(
                f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{path}",
                headers={"Authorization": f"token {GITHUB_TOKEN}",
                         "Accept": "application/vnd.github.v3+json"},
                json=payload,
                timeout=15
            )
            if r.status_code in (200, 201):
                log.info(f"[META] GitHub commit ✓ — {path}")
            else:
                log.warning(f"[META] GitHub commit failed {path}: {r.status_code} {r.text[:100]}")
        except Exception as e:
            log.error(f"[META] GitHub commit error: {e}")

    def _write_suggestions_to_github(self, claude_text: str, stats: dict, date_str: str):
        """Ask Claude to extract concrete suggestions and commit them to GitHub."""
        if not GITHUB_TOKEN:
            return
        try:
            suggestions_md = f"""# AlphaBot Suggested Changes — {date_str}

*Auto-generated by Meta Brain after market close*

## Performance Summary
- Trades: {stats.get('total', 0)} | Win rate: {stats.get('win_rate', 0):.1f}% | R:R: {stats.get('avg_rr', 0):.2f} | P&L: ${stats.get('total_pnl', 0):.2f}

## Claude Analysis & Suggestions
{claude_text}

---
*Review these suggestions before applying. Edit config.py directly or use the dashboard.*
"""
            self._commit_github_file(
                "suggested_changes.md",
                suggestions_md,
                f"meta brain suggestions {date_str}"
            )
        except Exception as e:
            log.error(f"[META] Failed to write suggestions to GitHub: {e}")

    def _save_review(self, stats, missed, issues, adjustments, report):
        """Save review to DB."""
        by_hour    = stats.get("by_hour", [])
        best_hour  = max(by_hour, key=lambda x: x["pnl"])["hour"]  if by_hour else None
        worst_hour = min(by_hour, key=lambda x: x["pnl"])["hour"]  if by_hour else None

        with get_conn() as conn:
            cur = conn.cursor()
            import uuid
            cur.execute("""
                INSERT INTO meta_reviews
                  (id, reviewed_at, win_rate_7d, avg_rr, best_hour,
                   worst_hour, missed_wins, top_issue, top_win,
                   adjustments, full_report)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                str(uuid.uuid4())[:8],
                datetime.utcnow(),
                stats.get("win_rate",  0),
                stats.get("avg_rr",    0),
                best_hour,
                worst_hour,
                missed.get("missed_wins", 0),
                issues,
                self._identify_top_win(stats),
                json.dumps(adjustments),
                report
            ))
