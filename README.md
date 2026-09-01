# MULTI7 SAFE67 — A / B / C / E + configurable NET take-profit

PAPER-only experiment on seven Polymarket 5-minute crypto chains:

```text
BTC
XRP
BNB
SOL
ETH
DOGE
HYPE (Hyperliquid)
```

There are **4 independent strategies per token = 28 PAPER accounts**.  
Each account starts at `$500` by default.

There is **no stop-loss** in any strategy.

## Configurable NET take-profit

All A/B/C/E PAPER positions now use one global `.env` setting:

```text
TAKE_PROFIT_USDC=0.30
```

`0.30` means: close the **entire remaining position** only when the currently
executable sell into visible bids would leave at least **+$0.30 total net PnL**.

The calculation includes:

```text
all entry gross cost
+ all entry commissions
- any prior net exit proceeds
- projected current sell proceeds
+ current exit commission
```

In formula form:

```text
NET PnL = prior exit net + current projected sell net - total buy cost
```

So the take-profit is **after both entry and exit commissions**.

Examples:

```text
TAKE_PROFIT_USDC=0.30   -> close at >= +$0.30 net
TAKE_PROFIT_USDC=0.50   -> close at >= +$0.50 net
TAKE_PROFIT_USDC=1.00   -> close at >= +$1.00 net
TAKE_PROFIT_USDC=OFF    -> disable
TAKE_PROFIT_USDC=0      -> disable
```

A decimal comma is also accepted, e.g. `0,30`.

The bot requires enough visible bid liquidity to sell **100% of the remaining
shares**. It does not partially close just to hit the target. When the local
book first appears to reach the target, the bid-side book is freshness-checked
and the full net PnL is recalculated before the PAPER sell is recorded.

For B/C after a DCA, the target is still **+$0.30 for the whole 10-share
position**, not $0.30 per share.

TP monitoring continues for already-open positions even when `STOP` blocks new
entries, so `STOP` does not strand an existing PAPER position.

A successful take-profit is written to:

```text
paper_exits.csv
reason = TAKE_PROFIT
```

and Telegram sends the actual average exit price and NET PnL.

## Common signal engine

All strategies keep the same first-V2 concept:

```text
V2 eligible:
price    0.55..0.75
momentum 0.03..0.30
lookback 2 decision ticks

decision loop ~3 seconds
trade window first 180 seconds
no side switching
no pre-decision REST refresh
```

The first V2-eligible signal decides that strategy's market gate permanently.

## A — SAFE67 BASE

```text
ENTRY price    0.67..0.75
ENTRY momentum 0.05..0.10
ENTRY size     5 shares

No DCA
No stop-loss
Max position 5 shares
```

This is the clean BASE control.

## B — SAFE67 REVERSAL DCA

The previous B logic is preserved:

```text
ENTRY price    0.67..0.75
ENTRY momentum 0.05..0.10
ENTRY size     5 shares

DCA arm:
held-side ask <= 0.50
elapsed <= 120 sec
NO BUY on the arming tick

Later rebound:
momentum >= +0.05
ask <= 0.60
DCA size = 5 shares
one DCA only
```

B deliberately has **no new 0.30 floor and no +0.15 momentum cap**. This keeps it as the old DCA control.

## C — TIGHT ENTRY + SAFER DCA

This is the new variant we agreed to test:

```text
ENTRY price    0.67..0.70
ENTRY momentum 0.05..0.10
ENTRY size     5 shares
```

DCA:

```text
arm when held-side ask <= 0.50
do not buy on the arm tick

on a later tick:
ask      0.30..0.60
momentum +0.05..+0.15
elapsed  <= 120 sec

DCA = 5 shares
one DCA only
max position = 10 shares
```

If a rebound happens below `0.30`, C does **not** buy it.  
If rebound momentum is above `+0.15`, C also does **not** buy it.

No stop-loss.

## E — SAFE67 CROSS-TOKEN CONSENSUS

E uses the same target-token entry threshold as A:

```text
target price    0.67..0.75
target momentum 0.05..0.10
ENTRY size      5 shares
```

But the entry is allowed only if, at the target's first SAFE67 decision:

```text
at least 2 DISTINCT OTHER tokens
had an A/BASE SAFE67 PASS
in the SAME direction
within the previous 10 seconds
```

Example:

```text
ETH A -> UP
SOL A -> UP
BTC E target -> UP
```

If both ETH and SOL votes are inside the 10-second window:

```text
BTC E -> BUY 5 shares
```

Rules:

- the target token itself never counts;
- one other token counts at most once;
- only **A/BASE SAFE67 PASS** is used as a vote;
- opposite-side signals do not count;
- if fewer than 2 confirmations exist at the first target SAFE67 decision, E skips that market permanently;
- E does not wait for confirmations to appear later;
- E has no DCA and no stop-loss.

The strategy loop is two-phase: all A/B/C decisions for all active tokens are processed first from the shared ~3-second WebSocket snapshot, then E is evaluated. This means genuinely simultaneous same-cycle A signals can count as consensus; signals that arrive in later cycles cannot revive an already-skipped E market.

## Hourly ZIP

One combined ZIP is sent each hour.

Root:

```text
variants_summary.csv
markets.csv
report.txt
```

Each token has four folders, for example:

```text
BTC/A_safe67_base_5sh/
BTC/B_safe67_reversal_dca_5plus5/
BTC/C_tight67_70_safer_dca_5plus5/
BTC/E_safe67_consensus_5sh/
```

The same structure exists for XRP, BNB, SOL, ETH, DOGE and HYPE.

Every strategy folder contains:

```text
summary.csv
gate_decisions.csv
paper_trades.csv
paper_exits.csv
dca_events.csv
consensus_events.csv
signals.csv
market_results.csv
position_trajectory.csv
report.txt
```

For E, `consensus_events.csv` records:

```text
target token
target side
target ask
target momentum
required confirmation count
actual confirmation count
confirming symbols
age of each confirmation in milliseconds
PASS/SKIP reason
```

This lets us later compare 2-vote vs 3-vote consensus and 5/10/15-second windows without guessing.

## Telegram

Buttons stay:

```text
START
STOP
BALANCE
STATISTICS
POSITIONS
TRADES
PAPER
LIVE
EMERGENCY STOP
```

LIVE is disabled in this experiment.

`STATISTICS` reports A/B/C/E separately for each token, including DCA armed/filled for B/C and consensus pass/checked for E.

## Render

Build:

```text
pip install -r requirements.txt
```

Start:

```text
python main.py
```

Persistent disk:

```text
/var/data
```

Fresh DB:

```text
/var/data/safe67_multi7_abce_takeprofit.db
```

Hourly reports:

```text
/var/data/safe67_multi7_abce_takeprofit_reports
```

After a fresh deploy trading starts OFF. Press `START`.

## Verification

`strategy_parity_check.txt` verifies that the uploaded A/B/C/E strategy remains
unchanged except for the take-profit hooks:

- `_strategy_set`, first-V2 selection, consensus source, market parsing,
  fee formula, buy/sell book simulation, momentum and DCA helpers are byte-exact;
- `evaluate_variant` and `evaluate_consensus_variant` differ only by the
  `take_profit_closed` early-return guard;
- the original buy path differs only by a guard preventing a new buy after a
  full TP exit;
- the strategy loop differs only by the TP monitor call.

Run both regressions:

```text
python test_multi7_abce.py
python test_takeprofit.py
```

Expected:

```text
MULTI7 SAFE67 A/B/C/E regression: OK
MULTI7 A/B/C/E configurable NET TAKE-PROFIT regression: OK
```

The TP regression explicitly checks:

- 5 shares bought at `0.68`;
- executable sell at `0.76` does **not** close because NET PnL is below `$0.30`;
- a high price with insufficient bid depth does **not** partially close;
- executable full sell at `0.77` closes with about `+$0.31186` NET;
- both entry and exit fees are included;
- the closed strategy cannot buy/DCA again;
- later market resolution does not add a second payout;
- hourly ZIP contains `paper_exits.csv` with `reason=TAKE_PROFIT`.
