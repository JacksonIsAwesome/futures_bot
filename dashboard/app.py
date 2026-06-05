"""
dashboard/app.py — Flask API + Dashboard Server for AlphaBot
"""

import json
import os
from flask import Flask, jsonify, request, send_from_directory
import psycopg2
import psycopg2.extras

app = Flask(__name__, static_folder='static')
DATABASE_URL = os.environ.get('DATABASE_URL', '')


def get_conn():
    return psycopg2.connect(DATABASE_URL)


@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


@app.route('/api/overview')
def overview():
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("SELECT * FROM trades WHERE status='open' ORDER BY entered_at")
            open_trades = [dict(r) for r in cur.fetchall()]
            for t in open_trades:
                t['entered_at'] = t['entered_at'].isoformat() if t['entered_at'] else None
            cur.execute("""
                SELECT COUNT(*) FILTER (WHERE status!='open') as closed,
                       COUNT(*) FILTER (WHERE pnl_usd>0) as wins,
                       COALESCE(SUM(pnl_usd) FILTER (WHERE status!='open'),0) as pnl,
                       COUNT(*) FILTER (WHERE status='open') as open_count
                FROM trades WHERE DATE(entered_at)=CURRENT_DATE
            """)
            today = dict(cur.fetchone())
            cur.execute("""
                SELECT COUNT(*) FILTER (WHERE status!='open') as total,
                       COUNT(*) FILTER (WHERE pnl_usd>0) as wins,
                       COALESCE(SUM(pnl_usd) FILTER (WHERE status!='open'),0) as total_pnl
                FROM trades WHERE entered_at>=NOW()-INTERVAL '7 days'
            """)
            week = dict(cur.fetchone())
            cur.execute("SELECT * FROM trades WHERE status!='open' ORDER BY exited_at DESC LIMIT 20")
            recent = [dict(r) for r in cur.fetchall()]
            for t in recent:
                t['entered_at'] = t['entered_at'].isoformat() if t['entered_at'] else None
                t['exited_at']  = t['exited_at'].isoformat()  if t['exited_at']  else None
            win_rate = round(today['wins']/today['closed']*100,1) if today['closed'] else 0
            week_wr  = round(week['wins']/week['total']*100,1)    if week['total']   else 0
            return jsonify({
                'open_trades': open_trades,
                'today': {'pnl': round(float(today['pnl'] or 0),2), 'trades': int(today['closed'] or 0),
                          'wins': int(today['wins'] or 0), 'win_rate': win_rate, 'open_count': int(today['open_count'] or 0)},
                'week':  {'pnl': round(float(week['total_pnl'] or 0),2), 'trades': int(week['total'] or 0), 'win_rate': week_wr},
                'recent_trades': recent
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/signals')
def signals():
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("SELECT * FROM signals ORDER BY timestamp DESC LIMIT 100")
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r['timestamp'] = r['timestamp'].isoformat() if r['timestamp'] else None
            return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/config', methods=['GET'])
def get_config():
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("SELECT key, value FROM config_overrides")
            overrides = {r['key']: r['value'] for r in cur.fetchall()}

        defaults = {
            # ── Base strategy ──────────────────────────────────
            'MIN_SIGNAL_SCORE':   3,
            'EMA_FAST':           9,
            'EMA_SLOW':           21,
            'RSI_OVERBOUGHT':     70,
            'RSI_OVERSOLD':       30,
            'VWAP_DEV_MULT':      1.5,
            'VOL_ACCEL_MULT':     1.8,
            # ── Session aggression ─────────────────────────────
            'PRIME_BASE_MIN':     3,
            'REGULAR_BASE_MIN':   4,
            'PRIME_END_HOUR':     11,
            # ── Momentum gate ──────────────────────────────────
            'MOMENTUM_GATE_ENABLED':       1,
            'MOMENTUM_GATE_MIN':           2,
            'ROC_PERIOD':                  3,
            'ROC_MIN_LONG':                0.08,
            'ROC_MIN_SHORT':               -0.08,
            'MACD_FAST':                   12,
            'MACD_SLOW':                   26,
            'MACD_SIGNAL_PERIOD':          9,
            'CANDLE_CONSISTENCY_LOOKBACK': 3,
            'CANDLE_CONSISTENCY_MIN':      2,
            # ── Multi-timeframe ────────────────────────────────
            'MTF_FILTER_ENABLED': 1,
            'MTF_EMA_PERIOD':     21,
            # ── Dynamic TP ─────────────────────────────────────
            'DYNAMIC_TP_ENABLED':      1,
            'DYNAMIC_TP_EXTENSION':    1.0,
            'DYNAMIC_TP_MIN_MOMENTUM': 2,
            # ── Faster scan ────────────────────────────────────
            'FAST_SCAN_ENABLED':  1,
            'FAST_SCAN_SCORE':    4,
            'FAST_SCAN_INTERVAL': 20,
            # ── ADX regime filter ──────────────────────────────
            'ADX_MIN_THRESHOLD':  20.0,
            # ── Direction flip ─────────────────────────────────
            'FLIP_ENABLED':        1,
            'FLIP_MIN_SIGNALS':    3,    # updated from 1
            'FLIP_BASE_SCORE_MIN': 4,    # updated from 3
            'FLIP_COOLDOWN_SEC':   600,  # new — 10 min between flips
            # ── Risk ───────────────────────────────────────────
            'SIMULATED_LEVERAGE': 10,
            'MAX_DAILY_LOSS_PCT': 0.30,
            'MAX_OPEN_TRADES':    7,
            'MAX_POSITION_PCT':   0.20,
            'RISK_PER_TRADE':     0.02,   # fraction of capital risked per trade
            # Sentiment
            'SENTIMENT_ENABLED':          1,
            'SENTIMENT_NEWS_ENABLED':     1,
            'SENTIMENT_GAP_ENABLED':      1,
            'SENTIMENT_VIX_ENABLED':      1,
            'SENTIMENT_NEWS_HOURS':       6,
            'SENTIMENT_NEWS_INFLUENCE':   0.30,
            'SENTIMENT_VIX_REDUCE':       20.0,   # VIX above this = reduce size
            'SENTIMENT_VIX_BLOCK':        28.0,   # VIX above this = no new entries
            'SENTIMENT_GAP_THRESHOLD':    0.3,    # gap % that matters
            'MORNING_CALL_ENABLED':       1,
            'ATR_STOP_MULT':      2.0,
            'ATR_TP_MULT':        4.0,
            'BREAKEVEN_ATR_MULT': 0.75,
            'TRAIL_STEP':         1.0,    # updated: wider trail (was 0.5)
            'STARTING_CAPITAL':   2000.0,
            'LOSS_COOLDOWN_MINS': 20,
            'MIN_RR':             1.0,
            # ── Session controls ───────────────────────────────
            'MORNING_BLACKOUT_ENABLED': 0,
            'MORNING_BLACKOUT_MINS':    10,
            'TRADING_PAUSED':     0,
            'CLOSE_EOD':          1,
            'BLACKOUT_ENABLED':   0,
            'BLACKOUT_START':     11,
            'BLACKOUT_END':       13,
        }
        for k, v in overrides.items():
            if k in defaults:
                try:
                    defaults[k] = float(v) if '.' in str(v) else int(v)
                except:
                    defaults[k] = v
        return jsonify(defaults)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/config', methods=['POST'])
def update_config():
    try:
        data = request.json
        with get_conn() as conn:
            cur = conn.cursor()
            for key, value in data.items():
                cur.execute("""
                    INSERT INTO config_overrides (key,value,updated_at) VALUES (%s,%s,NOW())
                    ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
                """, (key, str(value)))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/meta')
def meta_reviews():
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("SELECT * FROM meta_reviews ORDER BY reviewed_at DESC LIMIT 10")
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r['reviewed_at'] = r['reviewed_at'].isoformat() if r['reviewed_at'] else None
            return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/meta/run', methods=['POST'])
def run_meta():
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO config_overrides (key,value,updated_at) VALUES ('RUN_META_NOW','true',NOW())
                ON CONFLICT (key) DO UPDATE SET value='true', updated_at=NOW()
            """)
        return jsonify({'success': True, 'message': 'Meta brain queued — runs within 60s'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/health')
def health():
    """
    Returns bot health including Claude API status.
    Tracks both Haiku (per-signal direction calls) and Opus (morning call).
    """
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("""
                SELECT key, value, updated_at
                FROM config_overrides
                WHERE key IN (
                    'CLAUDE_API_FAILURES', 'CLAUDE_API_TOTAL', 'CLAUDE_LAST_SUCCESS',
                    'MORNING_CALL_ERROR', 'MORNING_CALL_DATE', 'MORNING_FULL_RESPONSE',
                    'MORNING_BIAS'
                )
            """)
            rows = {r['key']: {'value': r['value'], 'updated_at': r['updated_at'].isoformat() if r['updated_at'] else None}
                    for r in cur.fetchall()}

        # Haiku direction API health
        failures = int(rows.get('CLAUDE_API_FAILURES', {}).get('value', 0))
        total    = int(rows.get('CLAUDE_API_TOTAL',    {}).get('value', 1))
        last_ok  = rows.get('CLAUDE_LAST_SUCCESS', {}).get('updated_at')
        failure_rate = round(failures / max(total, 1) * 100, 1)
        status = "healthy" if failures < 3 else "degraded" if failures < 10 else "down"

        # Opus morning call health
        morning_error    = rows.get('MORNING_CALL_ERROR', {}).get('value', '')
        morning_date     = rows.get('MORNING_CALL_DATE',  {}).get('value', '')
        morning_ran      = bool(rows.get('MORNING_FULL_RESPONSE', {}).get('value', ''))
        morning_bias     = rows.get('MORNING_BIAS', {}).get('value', '')
        morning_ok       = morning_ran and not morning_error
        morning_status   = 'ok' if morning_ok else ('error' if morning_error else 'not_run')

        return jsonify({
            'claude_api': {
                'status':            status,
                'consecutive_fails': failures,
                'total_calls':       total,
                'failure_rate_pct':  failure_rate,
                'last_success':      last_ok,
                'using_fallback':    failures >= 3,
            },
            'opus_morning': {
                'status':   morning_status,
                'date':     morning_date,
                'bias':     morning_bias,
                'error':    morning_error or '',
                'ran_today': morning_ok,
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@app.route('/api/performance')
def performance():
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("""
                SELECT DATE(entered_at) as trade_date,
                       COALESCE(SUM(pnl_usd) FILTER (WHERE status!='open'),0) as pnl,
                       COUNT(*) FILTER (WHERE status!='open') as trades,
                       COUNT(*) FILTER (WHERE pnl_usd>0) as wins
                FROM trades WHERE entered_at>=NOW()-INTERVAL '30 days'
                GROUP BY DATE(entered_at) ORDER BY trade_date
            """)
            daily = [dict(r) for r in cur.fetchall()]
            for d in daily:
                d['trade_date'] = d['trade_date'].isoformat()
                d['pnl'] = round(float(d['pnl']),2)
            cur.execute("""
                SELECT symbol, COUNT(*) FILTER (WHERE status!='open') as trades,
                       COUNT(*) FILTER (WHERE pnl_usd>0) as wins,
                       COALESCE(SUM(pnl_usd) FILTER (WHERE status!='open'),0) as pnl
                FROM trades WHERE entered_at>=NOW()-INTERVAL '30 days'
                GROUP BY symbol ORDER BY pnl DESC
            """)
            by_symbol = [dict(r) for r in cur.fetchall()]
            for s in by_symbol:
                s['pnl'] = round(float(s['pnl']),2)
            return jsonify({'daily': daily, 'by_symbol': by_symbol})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/morning')
def morning():
    """Full morning call data including raw prompt, response, sentiment."""
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("""
                SELECT key, value, updated_at
                FROM config_overrides
                WHERE key IN (
                    'MORNING_BIAS','MORNING_FAVOR','MORNING_AVOID',
                    'MORNING_NOTES','MORNING_CALL_DATE','MORNING_FULL_PROMPT',
                    'MORNING_FULL_RESPONSE','MORNING_VIX','MORNING_VIX_ACTION',
                    'MORNING_GAPS','MORNING_HEADLINES','MORNING_CALL_ERROR',
                    'MORNING_SIZE_MULT'
                )
            """)
            rows = {r['key']: r['value'] for r in cur.fetchall()}
        # Parse JSON fields safely
        gaps      = {}
        headlines = {}
        try:    gaps      = json.loads(rows.get('MORNING_GAPS', '{}'))
        except: pass
        try:    headlines = json.loads(rows.get('MORNING_HEADLINES', '{}'))
        except: pass
        return jsonify({
            'bias':          rows.get('MORNING_BIAS', ''),
            'favor':         rows.get('MORNING_FAVOR', ''),
            'avoid':         rows.get('MORNING_AVOID', ''),
            'vix_action':    rows.get('MORNING_VIX_ACTION', 'normal'),
            'notes':         rows.get('MORNING_NOTES', ''),
            'date':          rows.get('MORNING_CALL_DATE', ''),
            'vix':           rows.get('MORNING_VIX', ''),
            'size_mult':     rows.get('MORNING_SIZE_MULT', '1.0'),
            'gaps':          gaps,
            'headlines':     headlines,
            'full_prompt':   rows.get('MORNING_FULL_PROMPT', ''),
            'full_response': rows.get('MORNING_FULL_RESPONSE', ''),
            'error':         rows.get('MORNING_CALL_ERROR', ''),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/morning/run', methods=['POST'])
def run_morning_call():
    """Manually trigger the morning call."""
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO config_overrides (key, value, updated_at, updated_by)
                VALUES ('RUN_MORNING_NOW', 'true', NOW(), 'dashboard')
                ON CONFLICT (key) DO UPDATE SET value='true', updated_at=NOW()
            """)
        return jsonify({'success': True, 'message': 'Morning call queued — runs within 60s'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sentiment')
def sentiment():
    """Live sentiment snapshot — VIX, gaps, headlines from this morning."""
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("""
                SELECT key, value, updated_at
                FROM config_overrides
                WHERE key IN (
                    'MORNING_VIX','MORNING_GAPS','MORNING_HEADLINES',
                    'MORNING_VIX_ACTION','MORNING_CALL_DATE'
                )
            """)
            rows = {r['key']: r['value'] for r in cur.fetchall()}
        gaps      = {}
        headlines = {}
        try:    gaps      = json.loads(rows.get('MORNING_GAPS', '{}'))
        except: pass
        try:    headlines = json.loads(rows.get('MORNING_HEADLINES', '{}'))
        except: pass
        return jsonify({
            'vix':        rows.get('MORNING_VIX', ''),
            'vix_action': rows.get('MORNING_VIX_ACTION', 'normal'),
            'date':       rows.get('MORNING_CALL_DATE', ''),
            'gaps':       gaps,
            'headlines':  headlines,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/exits')
def exits():
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("""
                SELECT exit_reason,
                       COUNT(*) as count,
                       COALESCE(SUM(pnl_usd),0) as total_pnl,
                       COALESCE(AVG(pnl_usd),0) as avg_pnl
                FROM trades
                WHERE status != 'open' AND entered_at >= NOW() - INTERVAL '30 days'
                GROUP BY exit_reason ORDER BY count DESC
            """)
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r['total_pnl'] = round(float(r['total_pnl']), 2)
                r['avg_pnl']   = round(float(r['avg_pnl']), 2)
            return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
