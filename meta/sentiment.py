"""
meta/sentiment.py
─────────────────
Pulls sentiment data for the morning call:
  1. Alpaca news headlines (per symbol, last N hours)
  2. Pre-market gap % per symbol (vs previous close)
  3. VIX level from Yahoo Finance

All reads are gated by DB config switches so the dashboard can
toggle each source independently.

Called by morning_call.py at 9:25am ET. Returns a SentimentData
dataclass that gets folded into the Opus prompt.
"""

import logging
import requests
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)


def _get_config(key, default):
    try:
        from core.database import get_config_override
        return get_config_override(key, default)
    except Exception:
        return default


def _get_alpaca_keys():
    try:
        from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
        return ALPACA_API_KEY, ALPACA_SECRET_KEY
    except Exception:
        import os
        return os.environ.get("ALPACA_API_KEY", ""), os.environ.get("ALPACA_SECRET_KEY", "")


@dataclass
class SentimentData:
    # News
    news_enabled:   bool = False
    headlines:      dict = field(default_factory=dict)  # symbol -> list of str
    news_hours:     int  = 6

    # Pre-market gap
    gap_enabled:    bool = False
    gaps:           dict = field(default_factory=dict)  # symbol -> float (%)

    # VIX
    vix_enabled:    bool = False
    vix:            Optional[float] = None

    # Computed
    vix_regime:     str  = "normal"   # "low" | "normal" | "elevated" | "fear"
    news_influence: float = 0.30      # 0.1–0.5, how much weight Opus gives news


# ── News ─────────────────────────────────────────────────────────────────────

def _fetch_alpaca_news(symbols: list, hours_back: int) -> dict:
    """
    Pull headlines from Alpaca News API.
    Returns dict: symbol -> [headline strings]
    """
    api_key, secret_key = _get_alpaca_keys()
    if not api_key:
        log.warning("[SENTIMENT] No Alpaca keys — skipping news")
        return {}

    start = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    results = {}

    try:
        resp = requests.get(
            "https://data.alpaca.markets/v1beta1/news",
            headers={
                "APCA-API-KEY-ID":     api_key,
                "APCA-API-SECRET-KEY": secret_key,
            },
            params={
                "symbols": ",".join(symbols),
                "start":   start,
                "limit":   50,
                "sort":    "desc",
            },
            timeout=10,
        )
        resp.raise_for_status()
        articles = resp.json().get("news", [])

        for article in articles:
            headline = article.get("headline", "").strip()
            if not headline:
                continue
            for sym in article.get("symbols", []):
                if sym in symbols:
                    if sym not in results:
                        results[sym] = []
                    if len(results[sym]) < 5:  # max 5 per symbol
                        results[sym].append(headline)

        total = sum(len(v) for v in results.values())
        log.info(f"[SENTIMENT] Fetched {total} headlines for {list(results.keys())}")

    except Exception as e:
        log.error(f"[SENTIMENT] Alpaca news fetch failed: {e}")

    return results


# ── Pre-market gap ────────────────────────────────────────────────────────────

def _fetch_gaps(symbols: list, stream_cache: dict) -> dict:
    """
    Calculate pre-market gap % for each symbol.
    Uses Alpaca bars API: compare latest pre-market price to prev close.
    Falls back to stream_cache price if bars unavailable.
    """
    api_key, secret_key = _get_alpaca_keys()
    gaps = {}

    for sym in symbols:
        try:
            resp = requests.get(
                f"https://data.alpaca.markets/v2/stocks/{sym}/bars",
                headers={
                    "APCA-API-KEY-ID":     api_key,
                    "APCA-API-SECRET-KEY": secret_key,
                },
                params={
                    "timeframe": "1Day",
                    "limit":     2,
                    "feed":      "iex",
                },
                timeout=8,
            )
            resp.raise_for_status()
            bars = resp.json().get("bars", [])
            if len(bars) >= 2:
                prev_close   = float(bars[-2]["c"])
                current_price = float(bars[-1]["c"])
                gap_pct = ((current_price - prev_close) / prev_close) * 100
                gaps[sym] = round(gap_pct, 3)
                log.info(f"[SENTIMENT] {sym} gap: {gap_pct:+.2f}%")
            elif stream_cache.get(sym, {}).get("price"):
                # fallback to stream price — gap unknown but at least we have price
                gaps[sym] = 0.0
        except Exception as e:
            log.debug(f"[SENTIMENT] Gap fetch failed for {sym}: {e}")
            gaps[sym] = 0.0

    return gaps


# ── VIX ───────────────────────────────────────────────────────────────────────

def _fetch_vix() -> Optional[float]:
    """Fetch VIX from Yahoo Finance quote endpoint."""
    try:
        resp = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX",
            headers={"User-Agent": "Mozilla/5.0"},
            params={"interval": "1d", "range": "1d"},
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()
        price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
        vix = round(float(price), 2)
        log.info(f"[SENTIMENT] VIX: {vix}")
        return vix
    except Exception as e:
        log.warning(f"[SENTIMENT] VIX fetch failed: {e}")
        return None


def _vix_regime(vix: Optional[float]) -> str:
    if vix is None:
        return "unknown"
    if vix < 15:
        return "low"
    if vix < 20:
        return "normal"
    if vix < 25:
        return "elevated"
    return "fear"


# ── Main entry ────────────────────────────────────────────────────────────────

def fetch(symbols: list, stream_cache: dict = None) -> SentimentData:
    """
    Fetch all enabled sentiment data. Respects DB config switches.
    Called by morning_call.py before building the Opus prompt.
    """
    if stream_cache is None:
        stream_cache = {}

    # Read switches from DB
    sentiment_enabled = int(_get_config("SENTIMENT_ENABLED", 1))
    news_enabled      = int(_get_config("SENTIMENT_NEWS_ENABLED", 1))
    gap_enabled       = int(_get_config("SENTIMENT_GAP_ENABLED", 1))
    vix_enabled       = int(_get_config("SENTIMENT_VIX_ENABLED", 1))
    news_hours        = int(_get_config("SENTIMENT_NEWS_HOURS", 6))
    news_influence    = float(_get_config("SENTIMENT_NEWS_INFLUENCE", 0.30))

    data = SentimentData(
        news_enabled   = bool(news_enabled and sentiment_enabled),
        gap_enabled    = bool(gap_enabled and sentiment_enabled),
        vix_enabled    = bool(vix_enabled and sentiment_enabled),
        news_hours     = news_hours,
        news_influence = news_influence,
    )

    if not sentiment_enabled:
        log.info("[SENTIMENT] Sentiment disabled — skipping all fetches")
        return data

    if data.news_enabled:
        data.headlines = _fetch_alpaca_news(symbols, news_hours)

    if data.gap_enabled:
        data.gaps = _fetch_gaps(symbols, stream_cache)

    if data.vix_enabled:
        data.vix = _fetch_vix()
        data.vix_regime = _vix_regime(data.vix)

    return data


def format_for_prompt(data: SentimentData, symbols: list) -> str:
    """
    Format SentimentData into a prompt block for Opus.
    Returns empty string if no sentiment data available.
    """
    sections = []

    if data.vix_enabled and data.vix is not None:
        regime_desc = {
            "low":      "calm market, momentum strategies tend to work well",
            "normal":   "normal conditions",
            "elevated": "some fear — prefer higher-conviction setups, reduce AVOID list",
            "fear":     "HIGH FEAR — seriously consider reducing all sizes, avoid leveraged ETFs",
        }.get(data.vix_regime, "unknown")
        sections.append(
            f"=== VIX ===\nVIX: {data.vix:.1f} ({data.vix_regime.upper()}) — {regime_desc}"
        )

    if data.gap_enabled and data.gaps:
        gap_lines = []
        for sym in symbols:
            g = data.gaps.get(sym)
            if g is not None:
                direction = "↑" if g > 0 else "↓" if g < 0 else "→"
                gap_lines.append(f"  {sym}: {direction}{abs(g):.2f}% gap")
        if gap_lines:
            sections.append("=== PRE-MARKET GAPS ===\n" + "\n".join(gap_lines))

    if data.news_enabled and data.headlines:
        news_lines = []
        for sym in symbols:
            headlines = data.headlines.get(sym, [])
            if headlines:
                news_lines.append(f"  {sym}:")
                for h in headlines[:3]:
                    news_lines.append(f"    • {h}")
        if news_lines:
            pct = int(data.news_influence * 100)
            sections.append(
                f"=== NEWS HEADLINES (last {data.news_hours}h) ===\n"
                + "\n".join(news_lines)
                + f"\n[Weight news at ~{pct}% of your decision, technicals at ~{100-pct}%]"
            )

    return "\n\n".join(sections)
