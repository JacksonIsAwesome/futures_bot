"""
core/error_monitor.py — Bot Error Detection + LLM Diagnosis

When the bot hits a crash or repeated scan errors, this:
  1. Collects the error + recent context
  2. Calls NVIDIA's Nemotron API to diagnose it (swapped from Claude)
  3. Texts you the diagnosis + suggested fix

Plugs into main.py's exception handler — no Railway API needed.
The bot catches its own errors from inside the process.
"""

import logging
import traceback
import os
from collections import deque
from datetime import datetime
from core.llm_client import call_llm, LLMError, DEEP_MODEL

log = logging.getLogger(__name__)

NVIDIA_API_KEY = os.environ.get("NVIDIA_API_KEY", "")

# Rolling buffer of recent log lines so the LLM has context
_recent_logs: deque = deque(maxlen=50)
_error_count: dict  = {}   # error_hash -> count (suppresses duplicate texts)


class LogCapture(logging.Handler):
    """Captures recent log lines into a rolling buffer."""
    def emit(self, record):
        try:
            _recent_logs.append(f"[{record.levelname}] {record.getMessage()}")
        except Exception:
            pass


def install():
    """Call once at bot startup to attach the log capture handler."""
    handler = LogCapture()
    handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(handler)
    log.info("[MONITOR] Error monitor installed ✓")


def _get_recent_logs() -> str:
    return "\n".join(list(_recent_logs)[-30:])


def _diagnose_with_llm(error: str, context: str) -> str:
    """Ask the LLM to diagnose the error and suggest a fix."""
    if not NVIDIA_API_KEY:
        return "NVIDIA API not configured."

    prompt = f"""You are debugging a Python algorithmic trading bot called AlphaBot.
It runs on Railway (cloud), uses Alpaca paper trading API, PostgreSQL, and WebSocket streams.

Here is the error that just crashed the bot:
{error}

Here are the last 30 log lines before the crash:
{context}

Diagnose the error in 2-3 sentences. Then give ONE specific fix —
a code change, env variable, or config value. Be concrete and brief.
If it's a known issue (connection timeout, stale data, API rate limit),
say so and whether the bot will auto-recover."""

    try:
        return call_llm(
            prompt=prompt,
            api_key=NVIDIA_API_KEY,
            model=DEEP_MODEL,
            max_tokens=300,
            timeout=20,
            temperature=0.3,
        )
    except LLMError as e:
        log.error(f"[MONITOR] LLM diagnosis failed: {e}")
        return f"LLM unavailable. Raw error: {error[:200]}"


def report_error(error: Exception, context: str = ""):
    """
    Call this when the bot hits an error worth reporting.
    Deduplicates — same error only texts you once per hour.
    """
    from core.notifier import _send  # local import to avoid circular

    error_str  = traceback.format_exc() or str(error)
    error_type = type(error).__name__
    log_ctx    = _get_recent_logs()

    # Deduplicate — same error type only once per hour
    now        = datetime.utcnow()
    last_seen  = _error_count.get(error_type)
    if last_seen and (now - last_seen).total_seconds() < 3600:
        log.debug(f"[MONITOR] Suppressing duplicate error text: {error_type}")
        return
    _error_count[error_type] = now

    log.error(f"[MONITOR] Reporting error to LLM for diagnosis: {error_type}")

    diagnosis = _diagnose_with_llm(
        error=error_str[:1500],
        context=log_ctx
    )

    msg = (
        f"⚠️ AlphaBot Error\n"
        f"{error_type}: {str(error)[:100]}\n\n"
        f"LLM says:\n{diagnosis[:400]}"
    )
    _send(msg)


def report_scan_errors(consecutive_errors: int, last_error: Exception):
    """Call when the bot has N consecutive scan failures."""
    if consecutive_errors in (3, 10, 25):  # text at 3, 10, 25 consecutive fails
        report_error(last_error, f"Consecutive scan failures: {consecutive_errors}")
