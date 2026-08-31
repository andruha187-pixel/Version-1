# MULTI7 FIRST-V2 CONSENSUS F/G/H/J — FAST +$0.10 PROFIT STOP

This is the same PAPER MULTI7 FIRST-V2 CONSENSUS F/G/H/J strategy logic, with the protective exit separated from the strategy decision cycle.

## What changed

- FIRST-V2, momentum, consensus, F/G/H/J entry rules and H DCA signal rules remain on `DECISION_INTERVAL=3.0`.
- Open positions are watched separately every `BREAKEVEN_WATCH_INTERVAL=0.25` seconds by default.
- The fast watcher only reads already-open positions and best executable bids. It does **not** create FIRST-V2 votes, momentum samples, ENTRY signals or DCA signals.
- Arming no longer forces an extra wait. If an executable bid satisfies both arming and stop conditions, the bot can arm and trigger in the same watcher pass.
- Per-position action locks prevent a 3-second ENTRY/DCA execution from racing a fast stop exit.
- Once profit protection is armed, H will not add a DCA afterward; protection owns that open position.
- `STOP` / `EMERGENCY STOP` block new ENTRY/DCA but protection remains active.
- Hourly ZIP reports remain disabled. Telegram buttons remain.

## Profit protection

Defaults:

```env
BREAKEVEN_STOP_ENABLED=1
BREAKEVEN_TRIGGER_MOVE=0.05
BREAKEVEN_MIN_PROFIT_USDC=0.10
BREAKEVEN_WATCH_INTERVAL=0.25
```

The two requested controls stay exactly in `.env` and can be changed later:

```env
BREAKEVEN_STOP_ENABLED=1
BREAKEVEN_TRIGGER_MOVE=0.05
```

The stop price is fee-adjusted to target **+$0.10 total modeled PnL per strategy position** after modeled entry and exit fees. For a normal 5-share fill at 0.68, the arm is 0.73 and the calculated +$0.10 floor is about 0.72906.

For H, if DCA happened before the profit protection armed, the calculation uses the full actual position cost basis. If protection has already armed, no later DCA is allowed.

This remains PAPER-only. Even with a 0.25-second watcher, a real market can gap through the stop or lose liquidity; therefore a real LIVE fill can never be guaranteed at the calculated floor.

## Storage

Fresh database for this version:

```text
/var/data/safe67_multi7_consensus_fghj_profit10_faststop.db
```

## Run

```bash
pip install -r requirements.txt
python main.py
```

## Verification

```bash
python test_faststop.py
```

Expected final line:

```text
FAST PROFIT-STOP regression: OK
```

`strategy_parity_check.txt` records that the strategy signal functions remain byte-for-byte identical to the previous profit10 build.
