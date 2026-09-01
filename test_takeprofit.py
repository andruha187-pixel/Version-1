import os
import time
import math
import asyncio
import tempfile
import importlib.util
import zipfile
from pathlib import Path

tmp = tempfile.mkdtemp(prefix="abce_tp_")
os.environ["DATA_DIR"] = tmp
os.environ["TELEGRAM_BOT_TOKEN"] = ""
os.environ["TELEGRAM_CHAT_ID"] = ""
os.environ["SYMBOLS"] = "BTC,XRP,BNB,SOL,ETH,DOGE,HYPE"
os.environ["PAPER_START_BALANCE"] = "500"
os.environ["TAKE_PROFIT_USDC"] = "0.30"

here = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("bot", here / "main.py")
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

assert bot.TAKE_PROFIT_USDC == 0.30
assert len(bot.STRATEGIES) == 28

# Parser convenience.
os.environ["TAKE_PROFIT_USDC"] = "OFF"
assert bot._take_profit_from_env() is None
os.environ["TAKE_PROFIT_USDC"] = "0"
assert bot._take_profit_from_env() is None
os.environ["TAKE_PROFIT_USDC"] = "0,45"
assert bot._take_profit_from_env() == 0.45
os.environ["TAKE_PROFIT_USDC"] = "0.30"


def set_book(asset, bid, ask, bid_size=100.0, ask_size=100.0):
    bot.books[asset] = {
        "bids": {float(bid): float(bid_size)},
        "asks": {float(ask): float(ask_size)},
        "received_ms": bot.now_ms(),
        "source": "test",
    }


slot = (int(time.time()) // 300) * 300
m = {
    "condition_id": "cid-btc-tp",
    "symbol": "BTC",
    "question": "Bitcoin Up or Down TP test",
    "slug": f"btc-updown-5m-{slot}",
    "start_ts": slot,
    "end_ts": slot + 300,
    "up_asset": "BTC-UP-TP",
    "down_asset": "BTC-DOWN-TP",
}
bot.markets[m["condition_id"]] = m
bot.persist_market(m)

A = bot.STRATEGIES_BY_SYMBOL["BTC"][0]

# ENTRY: 5 shares @ 0.68.
set_book(m["up_asset"], 0.67, 0.68)
set_book(m["down_asset"], 0.31, 0.32)
assert asyncio.run(bot.execute_paper(m["condition_id"], A, m["up_asset"], "Up", "ENTRY"))

pos = bot.position_totals(m["condition_id"], A["name"])
assert abs(pos["bought"] - 5.0) < 1e-9
assert abs(pos["remaining"] - 5.0) < 1e-9

# At 0.76 the executable NET result is only about +$0.26 after both fees.
set_book(m["up_asset"], 0.76, 0.77)
mark_076 = bot.projected_full_exit(m["condition_id"], A["name"])
assert mark_076 is not None
assert mark_076["total_pnl"] < 0.30
assert not asyncio.run(bot.maybe_take_profit(m, A, 60.0))
assert bot.position_totals(m["condition_id"], A["name"])["remaining"] > 4.999

# Even a very high bid cannot trigger a PARTIAL TP when full depth is absent.
set_book(m["up_asset"], 0.90, 0.91, bid_size=2.0)
assert bot.projected_full_exit(m["condition_id"], A["name"]) is None
assert not asyncio.run(bot.maybe_take_profit(m, A, 63.0))
assert bot.position_totals(m["condition_id"], A["name"])["remaining"] > 4.999

# At 0.77, full visible depth gives >+$0.30 NET, so the whole position closes.
set_book(m["up_asset"], 0.77, 0.78, bid_size=100.0)
mark_077 = bot.projected_full_exit(m["condition_id"], A["name"])
assert mark_077 is not None
assert mark_077["total_pnl"] >= 0.30
expected = mark_077["total_pnl"]

assert asyncio.run(bot.maybe_take_profit(m, A, 66.0))

pos_after = bot.position_totals(m["condition_id"], A["name"])
assert pos_after["remaining"] <= 1e-9
assert len(pos_after["exits"]) == 1
exit_row = pos_after["exits"][0]
assert exit_row["reason"] == "TAKE_PROFIT"
assert abs(float(exit_row["filled_shares"]) - 5.0) < 1e-9

# The persisted realized PnL equals exit NET - entry cost, i.e. includes BOTH fees.
with bot.db() as conn:
    result = conn.execute(
        "SELECT * FROM market_results WHERE condition_id=? AND variant=?",
        (m["condition_id"], A["name"]),
    ).fetchone()
assert result is not None
assert result["winning_outcome"] == "TAKE_PROFIT"
assert float(result["pnl"]) >= 0.30
assert abs(float(result["pnl"]) - expected) < 1e-8

buy_fee = sum(float(r["fee"]) for r in pos_after["buys"])
exit_fee = sum(float(r["fee"]) for r in pos_after["exits"])
assert buy_fee > 0 and exit_fee > 0
assert abs(
    float(result["pnl"])
    - (
        sum(float(r["gross_proceeds"]) for r in pos_after["exits"])
        - exit_fee
        - sum(float(r["gross_cost"]) for r in pos_after["buys"])
        - buy_fee
    )
) < 1e-8

# Cash is realized immediately and the strategy cannot buy/DCA again after TP.
assert abs(bot.paper_cash(A["name"]) - (500.0 + float(result["pnl"]))) < 1e-8
st = bot.get_variant_state(m["condition_id"], A)
assert st["take_profit_closed"]

set_book(m["up_asset"], 0.67, 0.68)
assert not asyncio.run(bot.execute_paper(m["condition_id"], A, m["up_asset"], "Up", "ENTRY"))
assert bot.position_totals(m["condition_id"], A["name"])["bought"] == 5.0

# Later market settlement must not add a payout or change the already-final TP result.
cash_before_settlement = bot.paper_cash(A["name"])
asyncio.run(bot.settle_market(m["condition_id"], m["up_asset"], "Up"))
assert abs(bot.paper_cash(A["name"]) - cash_before_settlement) < 1e-8
with bot.db() as conn:
    result2 = conn.execute(
        "SELECT * FROM market_results WHERE condition_id=? AND variant=?",
        (m["condition_id"], A["name"]),
    ).fetchone()
assert result2["winning_outcome"] == "TAKE_PROFIT"
assert abs(float(result2["pnl"]) - float(result["pnl"])) < 1e-8

# Hourly report exposes the exit row and TP count.
hour_start = (int(time.time()) // 3600) * 3600
zip_path, summaries = bot.make_report(hour_start, hour_start + 3600)
sa = next(x for x in summaries if x["variant"] == A["name"])
assert sa["take_profit_exits"] == 1
assert float(sa["take_profit_usdc"]) == 0.30

with zipfile.ZipFile(zip_path, "r") as z:
    names = set(z.namelist())
    assert "BTC/A_safe67_base_5sh/paper_exits.csv" in names
    exits_csv = z.read("BTC/A_safe67_base_5sh/paper_exits.csv").decode("utf-8-sig")
    assert "TAKE_PROFIT" in exits_csv

print("MULTI7 A/B/C/E configurable NET TAKE-PROFIT regression: OK")
print(f"Example 5sh @0.68 -> sell @0.77 NET PnL: ${float(result['pnl']):+.5f}")
