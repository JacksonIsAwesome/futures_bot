"""
meta/morning_call.py — v2
──────────────────────────
Runs at 9:25am ET. Calls Opus 4.8 with full pre-market context:
  - Per-symbol indicator snapshot (price, ADX, RSI, EMA state, ATR)
  - Sentiment: Alpaca news headlines, pre-market gaps, VIX (each toggleable)
  - Meta brain performance context (7-day win rate, recent notes)

DB keys written:
  MORNING_BIAS          → "long" | "short" | "neutral"
  MORNING_FAVOR         → comma-sep symbols (1.5x size)
  MORNING_AVOID         → comma-sep symbols (0.5x size)
  MORNING_NOTES         → Opus summary text
  MORNING_CALL_DATE     → ISO date
  MORNING_FULL_PROMPT   → full prompt sent to Opus (for dashboard display)
  MORNING_FULL_RESPONSE → full raw Opus response (for dashboard display)
  MORNING_VIX           → VIX value
  MORNING_GAPS          → JSON of {symbol: gap_pct}
  MORNING_HEADLINES     → JSON of {symbol: [headlines]}
  MORNING_CALL_ERROR    → error message if call failed
"""

import logging
import requests
import json
from datetime import datetime, timezone
import pytz

from meta.sentiment import fetch as fetch_sentiment, format_for_prompt

log = logging.getLogger(__name__)
ET  = pytz.timezone("America/New_York")
SYMBOLS = ["QQQ", "NVDA", "TQQQ", "SPY", "SOXL", "AMD", "TSLA"]


def _get_api_key():
    try:
        from config import ANTHROPIC_API_KEY
        return ANTHROPIC_API_KEY
    except Exception:
        import os
        return os.environ.get("ANTHROPIC_API_KEY", "")


def _get_config(key, default):
    try:
        from core.database import get_config_override
        return get_config_override(key, default)
    except Exception:
        return default


def _write_to_db(results: dict):
    try:
        import psycopg2
        from config import DATABASE_URL
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        for key, val in results.items():
            cur.execute("""
                INSERT INTO config_overrides (key, value, updated_at, updated_by)
                VALUES (%s, %s, NOW(), 'morning_call')
                ON CONFLICT (key) DO UPDATE
                SET value=EXCLUDED.value, updated_at=NOW(), updated_by='morning_call'
            """, (key, str(val)[:4000]))  # cap at 4000 chars for long prompts
        conn.commit()
        conn.close()
        log.info(f"[MORNING] Wrote {len(results)} keys to DB")
    except Exception as e:
        log.error(f"[MORNING] DB write failed: {e}")


def _get_meta_context() -> str:
    """Pull recent meta brain performance for Opus context."""
    try:
        import psycopg2, psycopg2.extras
        from config import DATABASE_URL
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT win_rate_7d, avg_rr, full_report
            FROM meta_reviews
            ORDER BY reviewed_at DESC LIMIT 1
        """)
        row = cur.fetchone()
        conn.close()
        if row:
            wr  = f"{row['win_rate_7d']:.1f}%" if row['win_rate_7d'] else "—"
            rr  = f"{row['avg_rr']:.2f}" if row['avg_rr'] else "—"
            return f"=== META BRAIN CONTEXT (last 7 days) ===\nWin rate: {wr} | Avg R:R: {rr}"
        return ""
    except Exception:
        return ""


def _build_prompt(stream_cache: dict, sentiment_block: str) -> str:
    now_et   = datetime.now(ET)
    date_str = now_et.strftime("%A %B %d, %Y")

    symbol_lines = []
    for sym in SYMBOLS:
        c = stream_cache.get(sym, {})
        if c:
            price     = c.get("price", 0)
            adx       = c.get("adx", 0)
            rsi       = c.get("rsi", 50)
            ema9      = c.get("ema_fast", 0)
            ema21     = c.get("ema_slow", 0)
            atr       = c.get("atr", 0)
            ema_state = "BULL" if ema9 > ema21 else "BEAR"
            symbol_lines.append(
                f"  {sym}: ${price:.2f} | ADX={adx:.1f} RSI={rsi:.1f} "
                f"EMA={ema_state} ATR={atr:.4f}"
            )
        else:
            symbol_lines.append(f"  {sym}: no pre-market data")

    meta_ctx = _get_meta_context()
    sentiment_section = f"\n\n{sentiment_block}" if sentiment_block else ""

    return f"""You are the pre-market session analyst for AlphaBot, a momentum day trading bot.

Today is {date_str}. Market opens in ~5 minutes.

=== PRE-MARKET INDICATOR SNAPSHOT ===
{chr(10).join(symbol_lines)}
{meta_ctx}{sentiment_section}

=== YOUR JOB ===
Decide which symbols to favor (1.5x size), avoid (0.5x size), and the overall session bias.
The bot trades intraday momentum on 5-min bars using VWAP, ADX, MACD, EMA signals.
Weight technicals heavily. Use news/sentiment only to break ties or flag obvious risks.

Respond ONLY with valid JSON — no markdown, no explanation outside the JSON:
{{
  "overall_bias": "long" | "short" | "neutral",
  "favor": ["SYM1", "SYM2"],
  "avoid": ["SYM3"],
  "vix_action": "normal" | "reduce_size" | "no_new_entries",
  "notes": "2-3 sentence reasoning covering technicals and any key news"
}}

Rules:
- favor: max 3 symbols, strongest pre-market setup (ADX trending, RSI directional, EMA aligned)
- avoid: symbols that look choppy, ranging, or have negative news risk — can be empty []
- vix_action: your recommendation based on VIX level and overall risk
- notes: plain English, max 300 chars, mention any specific news that affected your decision
- Only use symbols from: {', '.join(SYMBOLS)}"""


def run(stream_cache: dict = None) -> dict:
    if stream_cache is None:
        stream_cache = {}

    # Check master toggle
    enabled = int(_get_config("MORNING_CALL_ENABLED", 1))
    if not enabled:
        log.info("[MORNING] Morning call disabled — skipping")
        return {}

    api_key = _get_api_key()
    if not api_key:
        log.warning("[MORNING] No Anthropic API key — skipping morning call")
        return {}

    log.info("[MORNING] ═══════════════════════════════════════════")
    log.info("[MORNING] Running Opus 4.8 pre-market session call...")

    # 1. Fetch sentiment data
    sentiment = fetch_sentiment(SYMBOLS, stream_cache)
    sentiment_block = format_for_prompt(sentiment, SYMBOLS)

    # 2. Build prompt
    prompt = _build_prompt(stream_cache, sentiment_block)
    log.info(f"[MORNING] Prompt built ({len(prompt)} chars)")

    today = datetime.now(ET).date().isoformat()

    # Store sentiment data regardless of API result
    base_payload = {
        "MORNING_FULL_PROMPT":  prompt,
        "MORNING_CALL_DATE":    today,
        "MORNING_VIX":          str(sentiment.vix) if sentiment.vix else "",
        "MORNING_GAPS":         json.dumps(sentiment.gaps),
        "MORNING_HEADLINES":    json.dumps(sentiment.headlines),
    }

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         api_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-opus-4-8",
                "max_tokens": 500,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()["content"][0]["text"].strip()
        log.info(f"[MORNING] Opus response: {raw}")

        result     = json.loads(raw)
        bias       = result.get("overall_bias", "neutral")
        favor      = [s for s in result.get("favor", []) if s in SYMBOLS]
        avoid      = [s for s in result.get("avoid", []) if s in SYMBOLS]
        vix_action = result.get("vix_action", "normal")
        notes      = result.get("notes", "")[:500]

        db_payload = {
            **base_payload,
            "MORNING_BIAS":          bias,
            "MORNING_FAVOR":         ",".join(favor),
            "MORNING_AVOID":         ",".join(avoid),
            "MORNING_VIX_ACTION":    vix_action,
            "MORNING_NOTES":         notes,
            "MORNING_FULL_RESPONSE": raw,
            "MORNING_CALL_ERROR":    "",
        }
        _write_to_db(db_payload)

        # Apply VIX action to size multiplier if needed
        if vix_action == "reduce_size":
            _write_to_db({"MORNING_SIZE_MULT": "0.75"})
            log.info("[MORNING] VIX action: reduce_size → size mult 0.75x")
        elif vix_action == "no_new_entries":
            _write_to_db({"MORNING_SIZE_MULT": "0.0"})
            log.warning("[MORNING] VIX action: no_new_entries → blocking all entries")
        else:
            _write_to_db({"MORNING_SIZE_MULT": "1.0"})

        log.info(f"[MORNING] Bias={bias} | Favor={favor} | Avoid={avoid} | VIX={vix_action}")
        log.info(f"[MORNING] Notes: {notes}")
        log.info("[MORNING] ═══════════════════════════════════════════")

        return db_payload

    except json.JSONDecodeError as e:
        err = f"JSON parse failed: {e}"
        log.error(f"[MORNING] {err} — raw: {raw[:300]}")
        _write_to_db({**base_payload, "MORNING_CALL_ERROR": err,
                      "MORNING_FULL_RESPONSE": raw if 'raw' in dir() else ""})
        return {}
    except Exception as e:
        err = str(e)
        log.error(f"[MORNING] Opus call failed: {err}")
        _write_to_db({**base_payload, "MORNING_CALL_ERROR": err, "MORNING_FULL_RESPONSE": ""})
        return {}
