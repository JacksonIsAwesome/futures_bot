"""
meta/morning_call.py
────────────────────
Runs at 9:25am ET each day.
Calls Claude Opus 4.8 with today's pre-market context and asks which symbols
to favor/avoid and what overall bias to carry into the session.

Results are written to config_overrides as:
  MORNING_BIAS        → "long" | "short" | "neutral"
  MORNING_FAVOR       → comma-separated symbols to weight heavier (e.g. "QQQ,NVDA")
  MORNING_AVOID       → comma-separated symbols to reduce size on
  MORNING_NOTES       → free text summary (truncated to 500 chars)
  MORNING_CALL_DATE   → today's date ISO string

The strategy reads MORNING_BIAS and MORNING_FAVOR in ema_vwap.py to:
  - Slightly relax base score requirement for favored symbols (3→2 during prime)
  - Apply a 1.5x position size multiplier for favored symbols
  - Block entries on avoided symbols unless score is 4/4
"""

import logging
import requests
import json
from datetime import datetime, timezone
import pytz

log = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")

SYMBOLS = ["QQQ", "NVDA", "TQQQ", "SPY", "SOXL", "AMD", "TSLA"]


def _get_api_key():
    try:
        from config import ANTHROPIC_API_KEY
        return ANTHROPIC_API_KEY
    except Exception:
        import os
        return os.environ.get("ANTHROPIC_API_KEY", "")


def _get_conn():
    try:
        import psycopg2
        from config import DATABASE_URL
        return psycopg2.connect(DATABASE_URL)
    except Exception:
        return None


def _write_to_db(results: dict):
    conn = _get_conn()
    if not conn:
        log.warning("[MORNING] DB unavailable — skipping write")
        return
    try:
        cur = conn.cursor()
        for key, val in results.items():
            cur.execute("""
                INSERT INTO config_overrides (key, value, updated_at, updated_by)
                VALUES (%s, %s, NOW(), 'morning_call')
                ON CONFLICT (key) DO UPDATE
                SET value=EXCLUDED.value, updated_at=NOW(), updated_by='morning_call'
            """, (key, str(val)))
        conn.commit()
        log.info(f"[MORNING] Wrote {len(results)} keys to DB")
    except Exception as e:
        log.error(f"[MORNING] DB write failed: {e}")
    finally:
        conn.close()


def _build_prompt(stream_cache: dict) -> str:
    """
    Build the Opus prompt with whatever pre-market data we have.
    stream_cache: dict keyed by symbol → {price, adx, rsi, ema9, ema21, atr}
    """
    now_et = datetime.now(ET)
    date_str = now_et.strftime("%A %B %d, %Y")

    symbol_lines = []
    for sym in SYMBOLS:
        c = stream_cache.get(sym, {})
        if c:
            price = c.get("price", 0)
            adx   = c.get("adx", 0)
            rsi   = c.get("rsi", 50)
            ema9  = c.get("ema_fast", 0)
            ema21 = c.get("ema_slow", 0)
            ema_state = "BULL" if ema9 > ema21 else "BEAR"
            symbol_lines.append(
                f"  {sym}: price=${price:.2f} ADX={adx:.1f} RSI={rsi:.1f} EMA={ema_state}"
            )
        else:
            symbol_lines.append(f"  {sym}: no pre-market data")

    symbols_block = "\n".join(symbol_lines)

    return f"""You are the pre-market analyst for AlphaBot, a momentum day trading bot.

Today is {date_str}. Market opens in ~5 minutes.

=== PRE-MARKET INDICATOR SNAPSHOT ===
{symbols_block}

=== YOUR JOB ===
Based on pre-market momentum and indicator context, give a brief session bias for each symbol.
The bot trades intraday momentum on 5-min bars with VWAP, ADX, MACD, and EMA signals.

Answer in this exact JSON format (no markdown, no explanation outside the JSON):
{{
  "overall_bias": "long" | "short" | "neutral",
  "favor": ["SYM1", "SYM2"],
  "avoid": ["SYM3"],
  "notes": "1-2 sentence summary of your reasoning"
}}

Rules:
- favor = symbols with strongest pre-market setup (ADX trending, RSI directional, EMA aligned) — max 3
- avoid = symbols that look choppy or mixed — can be empty
- overall_bias = what direction most symbols lean into open
- notes = plain english, max 150 chars
- Only include symbols from: {', '.join(SYMBOLS)}"""


def run(stream_cache: dict = None) -> dict:
    """
    Main entry point. Call this at 9:25am ET.
    stream_cache: optional dict of live symbol data from stream module.
    Returns the parsed result dict.
    """
    if stream_cache is None:
        stream_cache = {}

    api_key = _get_api_key()
    if not api_key:
        log.warning("[MORNING] No API key — skipping morning call")
        return {}

    prompt = _build_prompt(stream_cache)
    log.info("[MORNING] ═══════════════════════════════════════════")
    log.info("[MORNING] Running Opus 4.8 pre-market session call...")

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-opus-4-8",
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()["content"][0]["text"].strip()
        log.info(f"[MORNING] Opus raw response: {raw}")

        # parse JSON
        result = json.loads(raw)
        bias   = result.get("overall_bias", "neutral")
        favor  = [s for s in result.get("favor", []) if s in SYMBOLS]
        avoid  = [s for s in result.get("avoid", []) if s in SYMBOLS]
        notes  = result.get("notes", "")[:500]

        today = datetime.now(ET).date().isoformat()

        db_payload = {
            "MORNING_BIAS":      bias,
            "MORNING_FAVOR":     ",".join(favor),
            "MORNING_AVOID":     ",".join(avoid),
            "MORNING_NOTES":     notes,
            "MORNING_CALL_DATE": today,
        }
        _write_to_db(db_payload)

        log.info(f"[MORNING] Bias={bias} | Favor={favor} | Avoid={avoid}")
        log.info(f"[MORNING] Notes: {notes}")
        log.info("[MORNING] ═══════════════════════════════════════════")

        return db_payload

    except json.JSONDecodeError as e:
        log.error(f"[MORNING] JSON parse failed: {e} — raw: {raw[:200]}")
        return {}
    except Exception as e:
        log.error(f"[MORNING] Opus call failed: {e}")
        return {}
