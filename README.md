# Powerwinner-Inspired Strategy Simulator v1

Paper-only strategy research bot.

It **does not copy Powerwinner** and it **does not place real orders**.

## Purpose

We observed several structural patterns in Powerwinner's BTC 5-minute activity:

- approximately 3-second decision cadence
- trading concentrated in the first ~180 seconds
- fixed-lot execution
- adding to a side after its contract price rises
- switching sides when momentum changes
- holding positions through resolution

This bot tests that family of ideas independently.

## What it does

1. Discovers live BTC Up/Down markets.
2. Subscribes to both outcome order books over the public CLOB WebSocket.
3. Samples ask prices every ~3 seconds.
4. Runs 8 candidate momentum/pyramiding variants simultaneously.
5. Simulates taker fills through the actual visible ask depth.
6. Applies Polymarket crypto taker fees.
7. Settles each variant after the market resolves.
8. Ranks variants by realized PnL and ROI.

## Variants

Examples:

- `M03_P08_L2`
  - first entry after +0.03 contract-price momentum
  - pyramid after +0.08 above previous buy
  - 2 ticks = ~6 second momentum lookback

- `M08_P12_L3`
  - first entry after +0.08
  - pyramid after +0.12
  - 3 ticks = ~9 second lookback

We deliberately test several variants instead of assuming one exact formula.

## Telegram ZIP

Every completed UTC hour:

- `variants_summary.csv`
- `paper_trades.csv`
- `signals.csv`
- `market_results.csv`
- `markets.csv`
- `report.txt`

Upload the ZIP files back to ChatGPT. We can then refine the parameter grid.

## Render

Use a **separate repository and separate Render service**.

Build:

`pip install -r requirements.txt`

Start:

`python main.py`

Environment variables required:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Recommended persistent disk mount:

`/var/data`

## Important

This is an experimental paper simulation. A profitable short run is not enough to establish a robust strategy. We need many resolved markets and should test out-of-sample periods before considering any real-money implementation.
