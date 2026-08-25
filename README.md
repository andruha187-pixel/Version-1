# Polymarket Highest Temperature — Paper Bot v2.2

**Paper trading only.** There is no wallet, private key, order signing, real order placement, or on-chain redeem call in this project.

The bot tracks Polymarket **Highest temperature in ...** markets and compares three independent virtual strategies:

- **T79** — live YES ask crosses from below 0.79 to 0.79 or higher
- **T84** — live YES ask crosses from below 0.84 to 0.84 or higher
- **T89** — live YES ask crosses from below 0.89 to 0.89 or higher

Default paper order: **$5 notional**. Default virtual balance: **$1,000 per strategy**.

## New in v2.2 — entry liquidity filter

Before every paper buy the bot now reads the public YES ask book and requires enough **near-best-ask depth** for a realistic $5 execution.

Defaults:

```text
ENTRY_LIQUIDITY_CHECK_ENABLED=true
MIN_ENTRY_LIQUIDITY_USD=5
MAX_ENTRY_SLIPPAGE=0.02
```

That means the bot only considers ask levels from the current best ask through **best ask + $0.02**. The combined dollar notional in that window must be at least $5, and the complete $5 paper order must still be fillable.

Example: if only $0.50 is offered around 0.79 and the next meaningful liquidity is at 0.95, the signal is recorded as `SKIPPED / INSUFFICIENT_NEAR_ASK_LIQUIDITY`; no paper position is opened. This prevents thin books from producing unrealistic entries.

The skipped signal remains in `signals.csv`, including available/required liquidity and the allowed price range in the reason field.

## Fixed in v2.1 — protection from false post-resolution entries

This build includes a regression fix for the Denver-style false redeem/entry sequence:

- embedded Gamma markets with `closed=true`, `active=false`, or `acceptingOrders=false` are not subscribed as new entry candidates;
- immediately before a paper buy or stop-loss exit, the bot re-checks Gamma market status and refuses execution when orders are explicitly no longer accepted;
- an exact terminal YES ask of `1.000` can never create a 0.79/0.84/0.89 crossing signal;
- after every CLOB WebSocket reconnect, the first fresh quote becomes a new baseline instead of being compared with a stale pre-disconnect quote;
- once a market has been explicitly resolved in this process, later CLOB quotes cannot create a new entry or stop-loss;
- the first quote for every newly seen market remains arming-only.

`endDate` is deliberately **not** treated as an exact trading cutoff because weather markets may continue accepting orders around/after that calendar marker. Explicit Polymarket order-acceptance/resolution status is used instead.

## New in v2

### Telegram buttons

The `/start` or `/help` command shows four inline buttons:

- **▶️ Старт** — enables new paper entries
- **⏹ Стоп** — pauses new paper entries
- **📂 Позиции** — shows currently open positions
- **📊 Отчёт** — sends current statistics and a ZIP report

The Start/Stop state is stored in SQLite and survives normal restarts/redeploys when the database is on the Render persistent disk.

**Important:** `⏹ Стоп` pauses only *new entries*. Existing positions are still monitored, the stop-loss stays active, and official market resolution/redeem processing continues.

### Paper stop-loss at $0.40

By default:

```text
STOP_LOSS_ENABLED=true
STOP_LOSS_PRICE=0.40
```

The trigger uses the executable side of the market: **YES best bid <= $0.40**.

When triggered, the bot simulates selling the entire YES position against the real public CLOB **bid depth**, highest bid first. It does **not** pretend the exit happened exactly at $0.40. If the market gaps below the stop, the simulated fill uses the actual available lower bids.

If the full small paper position cannot be sold from visible bid liquidity, the bot does not invent a fill; it leaves the position open and retries while the stop condition remains active.

Exit taker fees are included in realized PnL. A stopped trade is recorded with status `STOP_LOSS`, trigger bid, average exit price, gross proceeds, exit fee, net proceeds, and realized PnL.

## Simulation rules

1. A market already above a threshold when first observed is not bought. The first quote only arms the tracker.
2. Entry requires a live crossing: `previous ask < threshold <= new ask`.
3. T79/T84/T89 are independent portfolios.
4. Entry fills use real public CLOB ask depth.
5. Entry requires at least $5 of ask liquidity within $0.02 of the current best ask by default.
6. Far-away asks outside that slippage window are ignored.
7. Stop-loss exits use real public CLOB bid depth.
8. By default the whole $5 entry must be fillable; otherwise the signal is skipped.
9. Taker fees are included on entry and stop-loss exit.
10. Open PnL is marked at best bid.
11. Resolution is detected by WebSocket and also polled through Gamma as a fallback.
12. Open winning/losing positions not stopped out are virtually redeemed after official resolution.
13. Signals, fills, fees, stop exits, resolutions, payouts and PnL are stored in SQLite.

## Render deployment

Upload all files in this folder to a GitHub repository and create a Render Blueprint/Web Service.

`render.yaml` creates a separate service named `polymarket-weather-paper-bot-v2` with a persistent disk and uses:

- database: `/var/data/weather_paper_v2.db`
- reports: `/var/data/reports_v2`
- stop-loss: `$0.40`
- strategies: `0.79,0.84,0.89`
- order: `$5`

Add these secrets in Render:

```text
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

### If the old bot is still running

Do **not** run the old service and this new service at the same time with the **same Telegram bot token**. Both use Telegram `getUpdates` polling and they can consume each other's button/command updates.

Either:

- keep the old bot running and only deploy v2 after stopping it, or
- create a second Telegram bot/token for v2 if you want both services running simultaneously.

The Polymarket paper logic itself can run simultaneously; the conflict concerns Telegram polling with the same token.

## Other Telegram commands

- `/status` or `/stats` — balance, PnL, ROI, W/L, stop-loss count, drawdown
- `/open` — open positions
- `/last` — latest trades
- `/markets` — tracked events/markets
- `/report` — current status + ZIP report
- `/help` — show the button menu

When `HOURLY_REPORTS=true`, status and ZIP statistics are still sent every hour even if new entries are paused.

## ZIP report

Contains:

- `summary.csv`
- `trades.csv`
- `signals.csv`
- `markets.csv`
- `top_prices.csv`
- `equity_snapshots.csv`
- `about.txt`

The `trades.csv` file includes stop-loss exit details.

## Web dashboard

- `/` — dashboard, refreshes every 30 seconds
- `/healthz` — health check
- `/api/status` — JSON status

The dashboard shows whether new entries are RUNNING/STOPPED and the configured stop-loss.
