# AlphaBot — EMA/VWAP Momentum Trading Bot

Simple, clean day trading bot. Scans every 5 seconds.
Trades stocks using proven technical setups. Learns daily via meta brain.

## Strategy

5-signal scoring system. Trade fires when 3+ signals align:

1. **EMA Crossover** — 9 EMA crosses above/below 21 EMA
2. **VWAP Side** — Price above VWAP for longs, below for shorts  
3. **Volume Spike** — Current volume > 1.5x 20-bar average
4. **RSI Confirm** — Not overbought on longs, not oversold on shorts
5. **Price Action** — Higher highs + higher lows (long) or opposite (short)

**Stop loss:** ATR × 1.5 below entry  
**Take profit:** ATR × 3.0 above entry (2:1 R:R minimum)  
**Breakeven:** Stop moves to entry once up 10 points  

## Risk Controls

- 30% max daily loss ($600 on $2k) → bot shuts down for the day
- Max 7 open trades simultaneously
- Max 20% of capital per trade
- Never trades same symbol twice at once
- Side verification before every close (prevents phantom short bug)

## Meta Brain

Runs daily at 5pm ET. Analyzes last 7 days:
- Win rate by hour → finds best/worst trading times
- Missed opportunities → signals that would have worked
- Auto-adjusts signal threshold, ATR multipliers, volume requirements
- Writes daily report to DB and logs

## Setup

### 1. Alpaca Account
- Sign up at alpaca.markets (free)
- Get paper trading API keys
- Enable paper trading

### 2. Railway Setup
- Create new project
- Add PostgreSQL database
- Set environment variables:
  ```
  ALPACA_API_KEY=your_key
  ALPACA_SECRET_KEY=your_secret
  DATABASE_URL=your_postgresql_url
  ```

### 3. Deploy
- Push to GitHub repo
- Connect Railway to GitHub
- Deploy with `worker: python main.py`

### 4. config.py
Update these values before deploying:
```python
ALPACA_API_KEY    = "YOUR_KEY"
ALPACA_SECRET_KEY = "YOUR_SECRET"  
DATABASE_URL      = "YOUR_DB_URL"
STARTING_CAPITAL  = 2000.0
```

## File Structure

```
alphabot/
├── main.py                  ← main loop, ties everything together
├── config.py                ← all tunable parameters
├── requirements.txt
├── Procfile                 ← Railway deployment
├── core/
│   ├── database.py          ← all PostgreSQL operations
│   ├── data.py              ← Alpaca market data + indicators
│   └── execution.py         ← order placement
├── strategies/
│   └── ema_vwap.py          ← signal generation
├── risk/
│   └── manager.py           ← position sizing, stops, daily limits
└── meta/
    └── brain.py             ← daily learning and threshold adjustment
```

## Symbols Traded

Default watchlist (edit in config.py):
- SPY — S&P 500 ETF
- QQQ — Nasdaq ETF  
- NVDA, AAPL, TSLA, AMD, META

## Expected Performance (paper)

With $2,000 capital and 2:1 R:R:
- Win rate needed to be profitable: >40%
- Target: 55-65% win rate
- Expected: $50-200/day on good days, $0-50 on slow days
- $500-1000/day requires scaling capital to $10k-20k

## To-Do (future)

- [ ] Web dashboard (Flask + Railway)
- [ ] Telegram alerts on trades
- [ ] Backtest runner against historical data
- [ ] Add futures (MNQ/MES) once capital scales
- [ ] Pre-market scanner for gap plays
