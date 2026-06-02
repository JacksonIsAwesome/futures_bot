"""
core/database.py — All database operations for AlphaBot v2

v2 changes:
  - signals table: added roc_confirm, macd_confirm, candle_confirm,
    mtf_ok, momentum_score, roc_value, macd_histogram columns
  - init_db(): ALTER TABLE migration runs safely on existing DBs
  - log_signal(): accepts all new momentum signal fields

v2.1 changes:
  - open_trade(): added atr parameter (stored in DB for risk manager)
  - init_db(): migration adds atr column to trades table
"""

import os
import uuid
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, date
from contextlib import contextmanager
from config import DATABASE_URL

log = logging.getLogger(__name__)


@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all tables if they don't exist, and migrate existing ones."""
    with get_conn() as conn:
        cur = conn.cursor()

        # ── Trades ───────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id            TEXT PRIMARY KEY,
                symbol        TEXT NOT NULL,
                side          TEXT NOT NULL,
                qty           REAL NOT NULL,
                entry_price   REAL NOT NULL,
                exit_price    REAL,
                stop_loss     REAL NOT NULL,
                take_profit   REAL NOT NULL,
                breakeven_set BOOLEAN DEFAULT FALSE,
                status        TEXT DEFAULT 'open',
                exit_reason   TEXT,
                pnl_usd       REAL,
                pnl_pct       REAL,
                signal_score  INTEGER,
                entered_at    TIMESTAMPTZ NOT NULL,
                exited_at     TIMESTAMPTZ,
                strategy      TEXT DEFAULT 'ema_vwap_momentum'
            )
        """)

        # ── Signals log ───────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id             TEXT PRIMARY KEY,
                symbol         TEXT NOT NULL,
                timestamp      TIMESTAMPTZ NOT NULL,
                score          INTEGER NOT NULL,
                direction      TEXT,
                ema_cross      BOOLEAN,
                vwap_side      BOOLEAN,
                volume_spike   BOOLEAN,
                rsi_confirm    BOOLEAN,
                price_action   BOOLEAN,
                price          REAL,
                atr            REAL,
                traded         BOOLEAN DEFAULT FALSE,
                trade_id       TEXT,
                would_have_won BOOLEAN,
                -- v2: momentum signal columns
                roc_confirm    BOOLEAN,
                macd_confirm   BOOLEAN,
                candle_confirm BOOLEAN,
                mtf_ok         BOOLEAN,
                momentum_score INTEGER,
                roc_value      REAL,
                macd_histogram REAL
            )
        """)

        # ── Migration: add v2 columns to existing signals table ─
        migrations = [
            "ALTER TABLE signals ADD COLUMN IF NOT EXISTS roc_confirm    BOOLEAN",
            "ALTER TABLE signals ADD COLUMN IF NOT EXISTS macd_confirm   BOOLEAN",
            "ALTER TABLE signals ADD COLUMN IF NOT EXISTS candle_confirm BOOLEAN",
            "ALTER TABLE signals ADD COLUMN IF NOT EXISTS mtf_ok         BOOLEAN",
            "ALTER TABLE signals ADD COLUMN IF NOT EXISTS momentum_score INTEGER",
            "ALTER TABLE signals ADD COLUMN IF NOT EXISTS roc_value      REAL",
            "ALTER TABLE signals ADD COLUMN IF NOT EXISTS macd_histogram REAL",
            # v2.1: store entry ATR so risk manager uses correct value for
            # breakeven/trail/TP calculations even if ATR changes mid-trade
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS atr REAL",
        ]
        for sql in migrations:
            try:
                cur.execute(sql)
            except Exception as e:
                log.debug(f"[DB] Migration skipped (likely already exists): {e}")

        # ── Daily summary ─────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_summary (
                trade_date     DATE PRIMARY KEY,
                trades_total   INTEGER DEFAULT 0,
                trades_won     INTEGER DEFAULT 0,
                trades_lost    INTEGER DEFAULT 0,
                gross_pnl      REAL DEFAULT 0,
                net_pnl        REAL DEFAULT 0,
                win_rate       REAL DEFAULT 0,
                max_drawdown   REAL DEFAULT 0,
                signals_total  INTEGER DEFAULT 0,
                signals_missed INTEGER DEFAULT 0,
                killed         BOOLEAN DEFAULT FALSE,
                kill_reason    TEXT
            )
        """)

        # ── Meta brain reviews ────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta_reviews (
                id           TEXT PRIMARY KEY,
                reviewed_at  TIMESTAMPTZ NOT NULL,
                win_rate_7d  REAL,
                avg_rr       REAL,
                best_hour    INTEGER,
                worst_hour   INTEGER,
                missed_wins  INTEGER,
                top_issue    TEXT,
                top_win      TEXT,
                adjustments  JSONB,
                full_report  TEXT
            )
        """)

        # ── Config overrides ──────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS config_overrides (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                updated_by TEXT DEFAULT 'meta_brain'
            )
        """)

        # ── Symbol profiles ───────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS symbol_profiles (
                symbol         TEXT PRIMARY KEY,
                atr_stop_mult  REAL,
                atr_tp_mult    REAL,
                breakeven_mult REAL,
                volume_spike_mult REAL,
                min_atr_floor  REAL,
                min_atr_pct    REAL,
                notes          TEXT,
                updated_at     TIMESTAMPTZ
            )
        """)

        log.info("[DB] Database initialized ✓")


# ── Trade operations ──────────────────────────────────────────

def open_trade(symbol, side, qty, entry_price, stop_loss,
               take_profit, signal_score, signal_id=None, atr=None):
    """
    Insert a new open trade row. atr stores the entry-time ATR so the
    risk manager can use the correct value for breakeven/trail/TP
    calculations even after the live ATR changes mid-trade.
    """
    trade_id = str(uuid.uuid4())[:8]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO trades
              (id, symbol, side, qty, entry_price, stop_loss,
               take_profit, signal_score, atr, entered_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (trade_id, symbol, side, qty, entry_price,
              stop_loss, take_profit, signal_score, atr, datetime.utcnow()))
        if signal_id:
            cur.execute(
                "UPDATE signals SET traded=TRUE, trade_id=%s WHERE id=%s",
                (trade_id, signal_id)
            )
    return trade_id


def close_trade(trade_id, exit_price, exit_reason):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT entry_price, qty, side FROM trades WHERE id=%s AND status='open'",
            (trade_id,)
        )
        row = cur.fetchone()
        if not row:
            log.warning(f"[DB] No open trade found: {trade_id}")
            return None
        entry_price, qty, side = row
        if side == "long":
            pnl_usd = (exit_price - entry_price) * qty
        else:
            pnl_usd = (entry_price - exit_price) * qty
        pnl_pct = pnl_usd / (entry_price * qty) * 100
        cur.execute("""
            UPDATE trades SET
                exit_price = %s, exit_reason = %s,
                pnl_usd = %s, pnl_pct = %s,
                status = %s, exited_at = %s
            WHERE id = %s
        """, (exit_price, exit_reason,
              round(pnl_usd, 4), round(pnl_pct, 4),
              "stopped" if exit_reason == "stop" else "closed",
              datetime.utcnow(), trade_id))
        return round(pnl_usd, 4)


def set_breakeven(trade_id, new_stop):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE trades SET stop_loss=%s, breakeven_set=TRUE
            WHERE id=%s AND status='open'
        """, (new_stop, trade_id))


def update_stop_loss(trade_id, new_stop):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE trades SET stop_loss=%s
            WHERE id=%s AND status='open'
        """, (new_stop, trade_id))


def get_open_trades():
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM trades WHERE status='open' ORDER BY entered_at")
        return [dict(r) for r in cur.fetchall()]


def get_open_trade_for_symbol(symbol):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT * FROM trades WHERE symbol=%s AND status='open' LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_todays_pnl():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COALESCE(SUM(pnl_usd), 0) FROM trades
            WHERE DATE(exited_at) = CURRENT_DATE AND status != 'open'
        """)
        return cur.fetchone()[0]


def get_todays_trade_count():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trades WHERE DATE(entered_at) = CURRENT_DATE")
        return cur.fetchone()[0]


# ── Signal operations ─────────────────────────────────────────

def log_signal(symbol, score, direction, ema_cross, vwap_side,
               volume_spike, rsi_confirm, price_action, price, atr,
               roc_confirm=None, macd_confirm=None, candle_confirm=None,
               mtf_ok=None, momentum_score=None, roc_value=None,
               macd_histogram=None):
    sig_id = str(uuid.uuid4())[:8]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO signals
              (id, symbol, timestamp, score, direction,
               ema_cross, vwap_side, volume_spike, rsi_confirm,
               price_action, price, atr,
               roc_confirm, macd_confirm, candle_confirm,
               mtf_ok, momentum_score, roc_value, macd_histogram)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (sig_id, symbol, datetime.utcnow(), score, direction,
              ema_cross, vwap_side, volume_spike, rsi_confirm,
              price_action, price, atr,
              roc_confirm, macd_confirm, candle_confirm,
              mtf_ok, momentum_score, roc_value, macd_histogram))
    return sig_id


# ── Daily summary ─────────────────────────────────────────────

def upsert_daily_summary():
    today = date.today()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status != 'open'),
                COUNT(*) FILTER (WHERE pnl_usd > 0),
                COUNT(*) FILTER (WHERE pnl_usd <= 0 AND status != 'open'),
                COALESCE(SUM(pnl_usd) FILTER (WHERE status != 'open'), 0)
            FROM trades WHERE DATE(entered_at) = %s
        """, (today,))
        total, won, lost, gross = cur.fetchone()
        win_rate = (won / total * 100) if total > 0 else 0
        cur.execute("""
            INSERT INTO daily_summary
              (trade_date, trades_total, trades_won, trades_lost,
               gross_pnl, net_pnl, win_rate)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (trade_date) DO UPDATE SET
              trades_total = EXCLUDED.trades_total,
              trades_won   = EXCLUDED.trades_won,
              trades_lost  = EXCLUDED.trades_lost,
              gross_pnl    = EXCLUDED.gross_pnl,
              net_pnl      = EXCLUDED.net_pnl,
              win_rate     = EXCLUDED.win_rate
        """, (today, total, won, lost, gross, gross, win_rate))


# ── Config overrides ──────────────────────────────────────────

def get_config_override(key, default):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM config_overrides WHERE key=%s", (key,))
        row = cur.fetchone()
        if row:
            try:
                return type(default)(row[0])
            except Exception:
                return default
        return default


def set_config_override(key, value):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO config_overrides (key, value, updated_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (key) DO UPDATE SET
              value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
        """, (key, str(value), datetime.utcnow()))
