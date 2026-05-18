"""
meta/brain.py — Meta Brain

Runs daily after market close. Analyzes the last 7 days of trades
and signals to find patterns. Adjusts strategy thresholds based on
what's actually working vs what's being missed.

Uses Claude API for the daily report — feeds it the raw stats and
asks for a plain English analysis. Math still handles the threshold
adjustments so they're deterministic. Claude just explains what's
happening and gives one concrete suggestion.
"""

import json
import logging
import requests
import psycopg2.extras
from datetime import datetime, date, timedelta
from core.database import get_conn, set_config_override
import config

ANTHROPIC_API_KEY = config.ANTHROPIC_API_KEY  # add this to config.py

log = logging.getLogger(__name__)


class MetaBrain:
    def __init__(self):
        log.info("[META] Meta brain initialized ✓")

    def run_review(self):
        """Full daily review. Call this after market close."""
        log.info("[META] ═══════════════════════════════════════════")
        log.info("[META] Starting daily review...")

        stats    = self._gather_stats()
        missed   = self._find_missed_opportunities()
        issues   = self._identify_issues(stats)
        adjusts  = self._adjust_thresholds(stats, missed)
        report   = self._write_report(stats, missed, issues, adjusts)

        self._save_review(stats, missed, issues, adjusts, report)

        log.info("[META] Daily review complete ✓")
        log.info("[META] ═══════════════════════════════════════════")
        return report

    # ── Data gathering ────────────────────────────────────────

    def _gather_stats(self) -> dict:
        """Pull last 7 days of trade performance."""
        cutoff = datetime.utcnow() - timedelta(days=config.META_LOOKBACK_DAYS)

        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # overall stats
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status != 'open')          as total,
                    COUNT(*) FILTER (WHERE pnl_usd > 0)               as wins,
                    COUNT(*) FILTER (WHERE pnl_usd <= 0 AND status != 'open') as losses,
                    COALESCE(AVG(pnl_usd) FILTER (WHERE pnl_usd > 0), 0)  as avg_win,
                    COALESCE(AVG(pnl_usd) FILTER (WHERE pnl_usd <= 0 AND status != 'open'), 0) as avg_loss,
                    COALESCE(SUM(pnl_usd) FILTER (WHERE status != 'open'), 0) as total_pnl,
                    COALESCE(AVG(signal_score), 0)                    as avg_score
                FROM trades
                WHERE entered_at >= %s
            """, (cutoff,))
            row = dict(cur.fetchone())

            total = row["total"] or 0
            wins  = row["wins"]  or 0
            row["win_rate"] = (wins / total * 100) if total > 0 else 0

            # avg R:R
            avg_win  = abs(row["avg_win"])
            avg_loss = abs(row["avg_loss"])
            row["avg_rr"] = (avg_win / avg_loss) if avg_loss > 0 else 0

            # performance by hour
            cur.execute("""
                SELECT
                    EXTRACT(HOUR FROM entered_at AT TIME ZONE 'America/New_York') as hour,
                    COUNT(*) FILTER (WHERE pnl_usd > 0)  as wins,
                    COUNT(*) FILTER (WHERE pnl_usd <= 0 AND status != 'open') as losses,
                    COALESCE(SUM(pnl_usd), 0) as pnl
                FROM trades
                WHERE entered_at >= %s AND status != 'open'
                GROUP BY hour
                ORDER BY pnl DESC
            """, (cutoff,))
            row["by_hour"] = [dict(r) for r in cur.fetchall()]

            # performance by symbol
            cur.execute("""
                SELECT
                    symbol,
                    COUNT(*) FILTER (WHERE pnl_usd > 0) as wins,
                    COUNT(*) FILTER (WHERE pnl_usd <= 0 AND status != 'open') as losses,
                    COALESCE(SUM(pnl_usd), 0) as pnl
                FROM trades
                WHERE entered_at >= %s AND status != 'open'
                GROUP BY symbol
                ORDER BY pnl DESC
            """, (cutoff,))
            row["by_symbol"] = [dict(r) for r in cur.fetchall()]

            # exit reason breakdown
            cur.execute("""
                SELECT exit_reason, COUNT(*) as count,
                       COALESCE(SUM(pnl_usd), 0) as pnl
                FROM trades
                WHERE entered_at >= %s AND status != 'open'
                GROUP BY exit_reason
            """, (cutoff,))
            row["by_exit"] = [dict(r) for r in cur.fetchall()]

        return row

    def _find_missed_opportunities(self) -> dict:
        """
        Find high-score signals that weren't traded and
        determine if they would have been profitable.

        A signal is a "missed win" if:
          - score >= MIN_SIGNAL_SCORE but wasn't traded
          - OR score = MIN_SIGNAL_SCORE - 1 (just missed threshold)
          - and the price moved in the predicted direction afterward
        """
        cutoff = datetime.utcnow() - timedelta(days=config.META_LOOKBACK_DAYS)
        missed_wins  = 0
        missed_total = 0
        near_misses  = 0

        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # signals that weren't traded
            cur.execute("""
                SELECT * FROM signals
                WHERE timestamp >= %s
                  AND traded = FALSE
                  AND direction IS NOT NULL
                ORDER BY score DESC
            """, (cutoff,))
            untraded = cur.fetchall()
            missed_total = len(untraded)

            # count ones marked as would_have_won
            cur.execute("""
                SELECT COUNT(*) FROM signals
                WHERE timestamp >= %s
                  AND traded = FALSE
                  AND would_have_won = TRUE
            """, (cutoff,))
            missed_wins = cur.fetchone()[0]

            # signals that just missed the threshold
            cur.execute("""
                SELECT COUNT(*) FROM signals
                WHERE timestamp >= %s
                  AND traded = FALSE
                  AND score = %s
            """, (cutoff, config.MIN_SIGNAL_SCORE - 1))
            near_misses = cur.fetchone()[0]

        return {
            "total_untraded": missed_total,
            "missed_wins": missed_wins,
            "near_misses": near_misses,
            "miss_rate": (missed_wins / missed_total * 100) if missed_total > 0 else 0
        }

    def _identify_issues(self, stats: dict) -> str:
        """Identify the single biggest issue in simple English."""
        win_rate = stats.get("win_rate", 0)
        avg_rr   = stats.get("avg_rr", 0)
        total    = stats.get("total", 0)

        if total < config.META_MIN_TRADES:
            return f"Not enough trades yet ({total}/{config.META_MIN_TRADES} needed for analysis)"

        if win_rate < 40:
            return f"Win rate too low ({win_rate:.1f}%) — signals firing on weak setups"

        if avg_rr < 1.5:
            return f"R:R ratio too low ({avg_rr:.2f}) — take profits too tight or stops too wide"

        # find worst hour
        by_hour = stats.get("by_hour", [])
        if by_hour:
            worst = min(by_hour, key=lambda x: x["pnl"])
            if worst["pnl"] < -50:
                return f"Hour {int(worst['hour'])}:00 ET is consistently losing (${worst['pnl']:.0f})"

        # find worst symbol
        by_symbol = stats.get("by_symbol", [])
        if by_symbol:
            worst_sym = min(by_symbol, key=lambda x: x["pnl"])
            if worst_sym["pnl"] < -100:
                return f"{worst_sym['symbol']} is losing money (${worst_sym['pnl']:.0f}) — consider removing"

        return "No major issues identified — keep running"

    def _identify_top_win(self, stats: dict) -> str:
        """Identify the biggest thing working."""
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
        Nudge strategy thresholds based on performance.
        Small adjustments only — META_ADJUST_STEP at a time.
        Only adjusts after MIN_TRADES threshold is met.
        """
        adjustments = {}
        total = stats.get("total", 0)

        if total < config.META_MIN_TRADES:
            log.info(f"[META] Not enough trades for threshold adjustment ({total}/{config.META_MIN_TRADES})")
            return adjustments

        step     = config.META_ADJUST_STEP
        win_rate = stats.get("win_rate", 0)
        avg_rr   = stats.get("avg_rr", 0)
        missed_w = missed.get("missed_wins", 0)
        near_m   = missed.get("near_misses", 0)

        # ── If win rate is low, tighten signal requirements ───
        if win_rate < 45:
            current = config.MIN_SIGNAL_SCORE
            new_val = min(current + 1, 5)
            if new_val != current:
                set_config_override("MIN_SIGNAL_SCORE", new_val)
                adjustments["MIN_SIGNAL_SCORE"] = f"{current} → {new_val} (win rate too low)"

        # ── If win rate is high and we're missing a lot, loosen ─
        elif win_rate > 65 and missed_w > 5:
            current = config.MIN_SIGNAL_SCORE
            new_val = max(current - 1, 2)
            if new_val != current:
                set_config_override("MIN_SIGNAL_SCORE", new_val)
                adjustments["MIN_SIGNAL_SCORE"] = f"{current} → {new_val} (missing too many wins)"

        # ── If R:R is bad, widen take profits ─────────────────
        if avg_rr < 1.5 and total >= config.META_MIN_TRADES:
            current = config.ATR_TP_MULT
            new_val = round(current + step, 2)
            set_config_override("ATR_TP_MULT", new_val)
            adjustments["ATR_TP_MULT"] = f"{current} → {new_val} (R:R too low)"

        # ── If R:R is great, can tighten stops slightly ────────
        elif avg_rr > 3.0:
            current = config.ATR_STOP_MULT
            new_val = round(current - step, 2)
            if new_val >= 0.5:
                set_config_override("ATR_STOP_MULT", new_val)
                adjustments["ATR_STOP_MULT"] = f"{current} → {new_val} (R:R excellent, tightening stops)"

        # ── Volume threshold: if near misses are many, lower it ─
        if near_m > 10:
            current = config.VOLUME_SPIKE_MULT
            new_val = round(current - step, 2)
            if new_val >= 1.0:
                set_config_override("VOLUME_SPIKE_MULT", new_val)
                adjustments["VOLUME_SPIKE_MULT"] = f"{current} → {new_val} (too many near misses)"

        return adjustments

    def _write_report(self, stats, missed, issues, adjustments) -> str:
        """
        Call Claude API with the raw stats and get a plain English report.
        Falls back to a simple formatted report if the API call fails.
        """
        top_win = self._identify_top_win(stats)

        # build the data summary to send to Claude
        data_summary = {
            "date": str(date.today()),
            "lookback_days": config.META_LOOKBACK_DAYS,
            "performance": {
                "total_trades": stats.get("total", 0),
                "win_rate_pct": round(stats.get("win_rate", 0), 1),
                "avg_rr": round(stats.get("avg_rr", 0), 2),
                "total_pnl_usd": round(stats.get("total_pnl", 0), 2),
                "avg_signal_score": round(stats.get("avg_score", 0), 1),
            },
            "missed_opportunities": {
                "untraded_signals": missed.get("total_untraded", 0),
                "would_have_won": missed.get("missed_wins", 0),
                "near_misses": missed.get("near_misses", 0),
            },
            "by_hour": [
                {
                    "hour_et": int(h["hour"]),
                    "wins": h["wins"],
                    "losses": h["losses"],
                    "pnl": round(h["pnl"], 2)
                }
                for h in sorted(stats.get("by_hour", []), key=lambda x: x["hour"])
            ],
            "by_symbol": [
                {
                    "symbol": s["symbol"],
                    "wins": s["wins"],
                    "losses": s["losses"],
                    "pnl": round(s["pnl"], 2)
                }
                for s in sorted(stats.get("by_symbol", []),
                                key=lambda x: x["pnl"], reverse=True)
            ],
            "auto_adjustments_made": adjustments,
            "top_issue": issues,
            "top_win": top_win,
            "current_thresholds": {
                "min_signal_score": config.MIN_SIGNAL_SCORE,
                "volume_spike_mult": config.VOLUME_SPIKE_MULT,
                "atr_stop_mult": config.ATR_STOP_MULT,
                "atr_tp_mult": config.ATR_TP_MULT,
            }
        }

        prompt = f"""You are the meta brain of AlphaBot, an EMA/VWAP momentum day trading bot.

Here is today's performance data (last {config.META_LOOKBACK_DAYS} days):

{json.dumps(data_summary, indent=2)}

The bot uses 5 signals scored 0-5. A trade fires when score >= {config.MIN_SIGNAL_SCORE}.
Signals: EMA crossover, VWAP side, volume spike, RSI confirmation, price action.
Stop loss = ATR × {config.ATR_STOP_MULT}, Take profit = ATR × {config.ATR_TP_MULT}.

Write a short daily review. Keep it simple and direct. Include:
1. One sentence on overall performance
2. Best and worst hour to trade (with why, based on the data)
3. Best and worst symbol
4. One specific thing the bot is doing wrong
5. One specific thing the bot is doing right
6. One concrete suggestion for tomorrow (not already covered by auto-adjustments)

Keep the whole thing under 200 words. Write it like you're talking to a high school student
who built this bot and wants to understand what's happening. No jargon, no fluff."""

        try:
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 500,
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=30
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

        # build full report with stats header + Claude analysis
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

    def _save_review(self, stats, missed, issues, adjustments, report):
        """Save review to DB."""
        by_hour   = stats.get("by_hour", [])
        best_hour = max(by_hour, key=lambda x: x["pnl"])["hour"] if by_hour else None
        worst_hour = min(by_hour, key=lambda x: x["pnl"])["hour"] if by_hour else None

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
                stats.get("win_rate", 0),
                stats.get("avg_rr", 0),
                best_hour,
                worst_hour,
                missed.get("missed_wins", 0),
                issues,
                self._identify_top_win(stats),
                json.dumps(adjustments),
                report
            ))
