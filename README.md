# MULTI7 FIRST-V2 CONSENSUS F/G/H/J — +$0.10 PROFIT STOP

Same PAPER strategy engine as the MULTI7 FIRST-V2 CONSENSUS F/G/H/J regression bot. Entry, FIRST-V2, consensus, F/G/H/J and H DCA rules are unchanged. Hourly report generation is disabled; Telegram buttons remain.

## Profit protection

1. `BREAKEVEN_STOP_ENABLED=1` enables the protective exit.
2. `BREAKEVEN_TRIGGER_MOVE=0.05` keeps the original arm idea: normally the best executable bid must rise by +0.05 from the weighted gross entry average.
3. `BREAKEVEN_MIN_PROFIT_USDC=0.10` sets the default protected profit to **+$0.10 total per strategy position**, after modeled entry and exit crypto fees.
4. The bot calculates the sell price needed to leave that total PnL. For a normal 5-share fill at 0.68, arm is 0.73 and the fee-adjusted +$0.10 stop is about 0.7291.
5. For a rare partial entry fill where +0.05 is not enough to create +$0.10 total profit, the effective arm waits until the calculated profit-stop level is executable.
6. H uses the full actual position cost basis, including DCA if it happened before arming.
7. After the stop triggers, the bot keeps trying to flatten any unfilled remainder on later cycles.
8. `STOP` / `EMERGENCY STOP` block new ENTRY/DCA but do not disable protection of an already-open position.

This remains PAPER-only. A live market cannot guarantee the target profit because price gaps, slippage, missing liquidity, partial fills, and execution latency can produce a worse realized exit.

## Environment

The requested controls remain in `.env.example`:

```env
BREAKEVEN_STOP_ENABLED=1
BREAKEVEN_TRIGGER_MOVE=0.05
BREAKEVEN_MIN_PROFIT_USDC=0.10
```

You can change the trigger later without touching code. The profit target is also configurable.

Database: `/var/data/safe67_multi7_consensus_fghj_profit10.db` by default.

Run:

```bash
pip install -r requirements.txt
python main.py
```
