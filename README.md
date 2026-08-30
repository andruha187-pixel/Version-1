# MULTI7 FIRST-V2 CONSENSUS — F / G / H / J

PAPER-only experiment for seven Polymarket 5-minute crypto markets:

```text
BTC
XRP
BNB
SOL
ETH
DOGE
HYPE
```

There are **4 independent strategies per token = 28 PAPER accounts**.  
Default starting balance is `$500` for each account.

There is **no stop-loss** and **no side switching** in this build.

## Why this bot is separate

This bot isolates the cross-token idea found in the previous report analysis.

The important distinction is:

> A confirmation is the **FIRST V2-eligible signal** of another token, not a SAFE67 pass.

So another token can vote even when its own price/momentum would not pass the target SAFE entry.

## Common FIRST-V2 vote tape

For every token/market, the bot records exactly one first V2-eligible direction:

```text
price    0.55..0.75
momentum 0.03..0.30
lookback 2 decision ticks
```

Example:

```text
ETH first V2 vote: UP @ 0.60, momentum +0.04
```

This is a valid consensus vote even though `0.60 / +0.04` is not a SAFE target entry.

The target token itself never counts as its own confirmation.  
One other token counts at most once.

The default consensus window is the previous **10 seconds**.

All active tokens are sampled first, then their first-V2 votes are written using one shared decision-cycle timestamp, then F/G/H/J are evaluated. This avoids Python symbol iteration order deciding whether two signals in the same ~3-second cycle count. Same-cycle signals therefore count with age `0 ms`.

## F — TIGHT + 1 V2 confirmation

Target must have:

```text
price    0.67..0.70
momentum 0.05..0.10
```

and at least:

```text
1 DISTINCT OTHER token
same direction
FIRST V2-eligible vote
within previous 10 seconds
```

Then:

```text
ENTRY 5 shares
No DCA
No stop-loss
```

This is the higher-frequency consensus experiment.

## G — TIGHT + 2 V2 confirmations

Target:

```text
price    0.67..0.70
momentum 0.05..0.10
```

Consensus:

```text
>= 2 DISTINCT OTHER tokens
same direction
FIRST V2-eligible votes
within previous 10 seconds
```

Then:

```text
ENTRY 5 shares
No DCA
No stop-loss
```

This is the stricter combined C + consensus experiment.

## H — G + safer reversal DCA

H uses **exactly the same entry gate as G**:

```text
target 0.67..0.70
momentum 0.05..0.10
>=2 other same-side first-V2 votes / 10 sec
ENTRY 5 shares
```

After an actual entry, H may make one safer DCA.

Stage 1:

```text
held-side ask <= 0.50
elapsed <= 120 sec
=> DCA ARMED
```

There is **no buy on the arming tick**.

On a later decision tick:

```text
ask      0.30..0.60
momentum +0.05..+0.15
elapsed  <= 120 sec
```

Then:

```text
DCA +5 shares
one DCA only
max position 10 shares
```

If price is below `0.30`, H does not average.  
If rebound momentum is above `+0.15`, H does not average.  
No stop-loss.

## J — WIDE + 2 V2 confirmations

J is designed to reproduce the broader consensus rule separately from the tight entry:

```text
target price    0.67..0.75
target momentum 0.05..0.10

>=2 DISTINCT OTHER tokens
same direction
FIRST V2-eligible votes
within previous 10 seconds

ENTRY 5 shares
No DCA
No stop-loss
```

## What the comparison tells us

```text
F vs G:
Does requiring 2 confirmations improve win rate enough to justify fewer trades?

G vs H:
Does the safer DCA improve PnL after the exact same high-quality entries?

G vs J:
Does the tighter 0.67..0.70 target range improve results versus 0.67..0.75?

F vs J:
Frequency versus strictness.
```

## Hourly ZIP

One combined ZIP is sent each hour.

Root:

```text
variants_summary.csv
markets.csv
v2_votes.csv
report.txt
```

`v2_votes.csv` is especially important. It contains the raw first-V2 vote tape used by every strategy.

Each token has four folders, for example:

```text
BTC/F_tight67_70_one_v2_5sh/
BTC/G_tight67_70_two_v2_5sh/
BTC/H_tight67_70_two_v2_safe_dca_5plus5/
BTC/J_wide67_75_two_v2_5sh/
```

The same structure exists for XRP, BNB, SOL, ETH, DOGE and HYPE.

Each strategy folder contains:

```text
summary.csv
gate_decisions.csv
paper_trades.csv
dca_events.csv
consensus_events.csv
signals.csv
market_results.csv
position_trajectory.csv
report.txt
```

`consensus_events.csv` records:

```text
target token
target side
target ask
target momentum
window
required confirmation count
actual confirmation count
confirming symbols
age of every confirmation in milliseconds
PASS/SKIP reason
```

This makes later offline tests of 5/10/15-second windows and 1/2/3 confirmations possible without guessing.

## Telegram

Buttons:

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

LIVE is deliberately disabled.

## Render

Build command:

```text
pip install -r requirements.txt
```

Start command:

```text
python main.py
```

Persistent disk:

```text
/var/data
```

Fresh DB:

```text
/var/data/safe67_multi7_consensus_fghj.db
```

Reports:

```text
/var/data/safe67_multi7_consensus_fghj_reports
```

The fresh bot starts with trading OFF. Press `START`.

## Regression

Run:

```text
python test_multi7_fghj.py
```

Expected:

```text
MULTI7 FIRST-V2 CONSENSUS F/G/H/J regression: OK
```

The regression verifies, among other things:

- 28 independent accounts;
- F/G/H use target `0.67..0.70`;
- J uses `0.67..0.75`;
- confirmations come from FIRST V2-eligible votes, including votes that are not SAFE67 passes;
- with exactly 1 confirmation F can enter while G/H/J skip;
- with 2 confirmations a `0.72` target is rejected by F/G/H but accepted by J;
- H arms at `<=0.50`, refuses DCA below `0.30`, refuses rebound momentum above `+0.15`, and accepts a valid safer DCA;
- F/G/J never DCA;
- settlement remains token-scoped;
- hourly ZIP contains all 28 strategy folders and the raw `v2_votes.csv`.
