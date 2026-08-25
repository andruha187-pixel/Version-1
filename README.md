# Polymarket Highest Temperature — Paper Trading Bot

This bot is **paper trading only**. It contains no wallet, private key, signing code, real order placement, or on-chain redeem call.

It watches active Polymarket events whose title/slug matches **Highest temperature in ...** and runs three independent virtual strategies:

- T79: crosses from below 0.79 to 0.79 or higher
- T84: crosses from below 0.84 to 0.84 or higher
- T89: crosses from below 0.89 to 0.89 or higher

Default virtual order size: **$5 notional**. Default balance: **$1,000 per strategy**.

## Important simulation rules

1. A market already above a threshold when the bot first sees it is **not bought**. The first quote only arms the tracker.
2. A trade occurs only after a live crossing: `previous ask < threshold <= new ask`.
3. Each strategy can trade a particular binary temperature market only once.
4. The fill is simulated against the public CLOB **ask depth**, not the displayed midpoint.
5. By default the full $5 must be fillable; otherwise the signal is logged as skipped.
6. Taker fees are included. Per-market CLOB fee details are queried when possible; current Weather fee rate is used as a fallback.
7. Open PnL is marked at **best bid**, i.e. the realistic liquidation side.
8. Resolution is detected by WebSocket when available and also polled through Gamma as a fallback.
9. On resolution the bot performs a **virtual redeem** immediately: YES shares settle to the resolved YES value (normally $1 or $0).
10. Every signal, fill, fee, resolution, payout and PnL stays in SQLite.

## Files

- `main.py` — complete bot, dashboard, Telegram commands, reports
- `requirements.txt` — Python dependencies
- `.env.example` — settings
- `render.yaml` — Render Blueprint including a 1 GB persistent disk

## Render deployment

1. Upload these files to a GitHub repository.
2. In Render, create a Blueprint/Web Service from the repository (or create a Python Web Service manually).
3. Add `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` environment variables if you want Telegram notifications/reports.
4. Keep the persistent disk mounted at `/var/data`. SQLite and reports must live there or they will be lost on redeploy.
5. Deploy.

`render.yaml` already uses:

- build: `pip install -r requirements.txt`
- start: `python main.py`
- health check: `/healthz`
- database: `/var/data/weather_paper.db`

## Telegram commands

- `/status` — balances, PnL, ROI, W/L, drawdown
- `/stats` — strategy comparison
- `/open` — open positions
- `/last` — latest trades
- `/markets` — number of tracked city/date events and YES markets
- `/report` — generate and send a ZIP report immediately
- `/help` — commands

When `HOURLY_REPORTS=true`, the bot sends a status plus ZIP report every hour.

## ZIP report contents

- `summary.csv`
- `trades.csv`
- `signals.csv`
- `markets.csv`
- `top_prices.csv`
- `equity_snapshots.csv`
- `about.txt`

## Web dashboard

Open the Render service URL. The dashboard refreshes every 30 seconds and shows all three strategies side-by-side.

Endpoints:

- `/` — dashboard
- `/healthz` — health check
- `/api/status` — JSON status

If you set `DASHBOARD_TOKEN`, open the dashboard as `/?token=YOUR_TOKEN`.

## Strategy comparison

The three portfolios are deliberately independent. A move from 0.78 to 0.90 can therefore trigger T79, T84 and T89 at the same live order-book state, which makes the eventual PnL/ROI comparison fair instead of making the strategies compete for one shared virtual bankroll.
