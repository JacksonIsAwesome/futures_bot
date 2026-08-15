"""
core/llm_client.py — Shared NVIDIA NIM API client (Claude replacement)

Swapped from Anthropic to NVIDIA's free NIM API (build.nvidia.com) — free
tier, OpenAI-compatible endpoint, no card required. One shared client used
by every call site that used to hit api.anthropic.com directly:
  - strategies/ema_vwap.py   (direction gate)
  - meta/brain.py            (daily review)
  - meta/morning_call.py     (premarket bias)
  - meta/symbol_profiler.py  (per-symbol config tuning)
  - core/error_monitor.py    (crash diagnosis)

IMPORTANT — free tier rate limit: NVIDIA's community-acknowledged baseline
is ~40 requests/minute PER MODEL (not officially published, but consistently
reported across their dev forums as of mid-2026). The direction gate in
ema_vwap.py can fire once per symbol per scan cycle — with 8 symbols on a
5s scan interval, that's up to 96 calls/min if every symbol clears the ADX
gate at once. That WILL exceed 40 RPM on active trending days.

This isn't fatal: every call site already has a fallback that fires
automatically on ANY failure (429 included) — ema_vwap.py's rule-based
scored system, brain/morning_call's raw-stat text, symbol_profiler's
skip-and-keep-defaults, error_monitor's raw-error passthrough. Hitting the
limit just means the bot leans on those fallbacks more often; it doesn't
break. Watch for repeated "[CLAUDE-DIR] rate limited" log lines — if it's
constant rather than occasional, that's the signal to add a client-side
throttle (e.g. skip the API call for symbols after the Nth per scan cycle).

Models:
  FAST_MODEL — nvidia/llama-3.1-nemotron-nano-8b-v1
    Direction gate only. Called most often, needs low latency, plain
    instruct model (no reasoning-trace wrapping to worry about).
  DEEP_MODEL — nvidia/llama-3.3-nemotron-super-49b-v1.5
    Daily review, morning call, symbol profiler, error diagnosis — all
    low-frequency (daily or on-error), can afford a bigger model.
"""

import logging
import requests

log = logging.getLogger(__name__)

NVIDIA_API_URL = "https://integrate.api.nvidia.com/v1/chat/completions"

FAST_MODEL = "nvidia/llama-3.1-nemotron-nano-8b-v1"
DEEP_MODEL = "nvidia/llama-3.3-nemotron-super-49b-v1.5"


class LLMError(Exception):
    """Raised when the NVIDIA API call fails or returns something unusable.
    Every caller in this bot already has a fallback path for this."""
    pass


def call_llm(prompt: str, api_key: str, model: str = DEEP_MODEL,
             max_tokens: int = 300, timeout: int = 20,
             temperature: float = 0.3) -> str:
    """
    Call NVIDIA's NIM API (OpenAI-compatible chat completions).
    Returns the raw text response (with any <think> block stripped).
    Raises LLMError on any failure — caller handles fallback.
    """
    if not api_key:
        raise LLMError("No NVIDIA API key configured")

    try:
        r = requests.post(
            NVIDIA_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       model,
                "messages":    [{"role": "user", "content": prompt}],
                "max_tokens":  max_tokens,
                "temperature": temperature,
            },
            timeout=timeout,
        )
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"]
        return _strip_thinking(text).strip()

    except requests.exceptions.Timeout:
        raise LLMError(f"timeout ({timeout}s)")
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        if status == 429:
            raise LLMError("rate limited (429) — NVIDIA free tier ~40 req/min")
        raise LLMError(f"HTTP {status}: {e}")
    except (KeyError, IndexError):
        raise LLMError("unexpected response shape from NVIDIA API")
    except Exception as e:
        raise LLMError(str(e)[:200])


def _strip_thinking(text: str) -> str:
    """
    Some NVIDIA-hosted models can wrap internal reasoning in
    <think>...</think> before the real answer. Strip it so callers get a
    clean response. No-op for models that don't use it (FAST_MODEL/
    DEEP_MODEL as configured above are plain instruct models and shouldn't
    hit this, but it's cheap insurance if you swap models later).
    """
    if "<think>" in text and "</think>" in text:
        end = text.index("</think>") + len("</think>")
        text = text[end:]
    return text


def extract_json(text: str) -> str:
    """
    Pull a JSON object out of a response that might have markdown fences
    or stray prose around it. Returns the substring from the first '{' to
    the last '}'. Raises ValueError if no braces found — caller should
    treat that the same as any other parse failure.
    """
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    start = text.find("{")
    end   = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"No JSON object found in response: {text[:200]}")
    return text[start:end + 1]


def extract_word(text: str, valid_words: tuple) -> str:
    """
    Pull a single-word answer (e.g. 'long'/'short'/'none') out of a
    response that might have extra whitespace or punctuation around it.
    Returns the first valid word found. Raises ValueError if none match.
    """
    cleaned = text.strip().lower().rstrip(".")
    if cleaned in valid_words:
        return cleaned
    for word in cleaned.split():
        word = word.strip(".,!?\"'")
        if word in valid_words:
            return word
    raise ValueError(f"No valid word in {valid_words} found in response: {text[:100]}")
