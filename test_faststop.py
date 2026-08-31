import os
import time
import asyncio
import tempfile
import importlib.util
from pathlib import Path

TMP = tempfile.mkdtemp(prefix="multi7_faststop_")
os.environ["DATA_DIR"] = TMP
os.environ["TELEGRAM_BOT_TOKEN"] = ""
os.environ["TELEGRAM_CHAT_ID"] = ""
os.environ["SYMBOLS"] = "BTC,XRP,BNB,SOL,ETH,DOGE,HYPE"
os.environ["PAPER_START_BALANCE"] = "500"
os.environ["DECISION_INTERVAL"] = "3.0"
os.environ["BREAKEVEN_STOP_ENABLED"] = "1"
os.environ["BREAKEVEN_TRIGGER_MOVE"] = "0.05"
os.environ["BREAKEVEN_MIN_PROFIT_USDC"] = "0.10"
os.environ["BREAKEVEN_WATCH_INTERVAL"] = "0.25"

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("fastbot", HERE / "main.py")
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

assert bot.DECISION_INTERVAL == 3.0
assert bot.BREAKEVEN_WATCH_INTERVAL == 0.25
assert bot.BREAKEVEN_TRIGGER_MOVE == 0.05
assert bot.BREAKEVEN_MIN_PROFIT_USDC == 0.10
assert len(bot.STRATEGIES) == 28

F = bot.STRATEGIES_BY_SYMBOL["BTC"][0]
assert F["code"] == "F"


def add_market(tag, ask_qty=5.0):
    cid = f"cid-{tag}"
    up = f"up-{tag}"
    down = f"down-{tag}"
    start = int(time.time()) - 60
    m = {
        "condition_id": cid,
        "symbol": "BTC",
        "question": "test",
        "slug": f"btc-updown-5m-{start}",
        "start_ts": start,
        "end_ts": start + 300,
        "up_asset": up,
        "down_asset": down,
    }
    bot.markets[cid] = m
    bot.persist_market(m)
    now = bot.now_ms()
    bot.books[up] = {
        "bids": {0.67: 100.0},
        "asks": {0.68: ask_qty},
        "received_ms": now,
        "source": "test",
    }
    bot.books[down] = {
        "bids": {0.31: 100.0},
        "asks": {0.32: 100.0},
        "received_ms": now,
        "source": "test",
    }
    return m


def set_bid(asset, price, qty=100.0):
    b = bot.books[asset]
    b["bids"] = {float(price): float(qty)}
    b["received_ms"] = bot.now_ms()


async def normal_five_share_test():
    m = add_market("normal", 5.0)
    ok = await bot.execute_paper(m["condition_id"], F, m["up_asset"], "Up", "ENTRY")
    assert ok
    pos = bot.position_totals(m["condition_id"], F["name"])
    assert abs(pos["remaining"] - 5.0) < 1e-9
    assert abs(bot.weighted_gross_entry_avg(pos) - 0.68) < 1e-9

    # Fast watcher arms at +0.05, independently of the 3-second strategy cadence.
    set_bid(m["up_asset"], 0.73)
    await bot.breakeven_watch_once()
    ev = bot.breakeven_event(m["condition_id"], F["name"])
    assert ev is not None
    assert ev["triggered_ms"] is None
    stop = float(ev["stop_price"])
    assert 0.728 < stop < 0.731, stop

    # Next fast pass at the profit floor exits the position.
    set_bid(m["up_asset"], stop)
    await bot.breakeven_watch_once()
    ev = bot.breakeven_event(m["condition_id"], F["name"])
    assert ev["triggered_ms"] is not None
    assert ev["completed_ms"] is not None
    pos = bot.position_totals(m["condition_id"], F["name"])
    assert pos["remaining"] <= 1e-8
    pnl = pos["exit_net"] - pos["buy_cost"]
    assert pnl >= 0.09998, pnl
    return stop, pnl


async def same_pass_arm_trigger_partial_test():
    # One-share partial fill needs a much higher profit floor than entry+0.05.
    # At exactly that floor, the new code must ARM and TRIGGER in the SAME watcher pass.
    m = add_market("partial", 1.0)
    ok = await bot.execute_paper(m["condition_id"], F, m["up_asset"], "Up", "ENTRY")
    assert ok
    pos = bot.position_totals(m["condition_id"], F["name"])
    assert abs(pos["remaining"] - 1.0) < 1e-9
    stop = bot.fee_adjusted_profit_stop_price(pos)
    assert stop is not None and stop > 0.73
    assert bot.breakeven_event(m["condition_id"], F["name"]) is None

    set_bid(m["up_asset"], stop)
    await bot.breakeven_watch_once()
    ev = bot.breakeven_event(m["condition_id"], F["name"])
    assert ev is not None
    assert ev["triggered_ms"] is not None, "must trigger on the arming watcher pass"
    assert ev["completed_ms"] is not None
    pos2 = bot.position_totals(m["condition_id"], F["name"])
    assert pos2["remaining"] <= 1e-8
    pnl = pos2["exit_net"] - pos2["buy_cost"]
    assert pnl >= 0.09998, pnl
    return stop, pnl


async def main():
    s1, p1 = await normal_five_share_test()
    s2, p2 = await same_pass_arm_trigger_partial_test()
    print(f"normal 5sh stop={s1:.6f} pnl={p1:+.6f}")
    print(f"partial 1sh same-pass stop={s2:.6f} pnl={p2:+.6f}")
    print("FAST PROFIT-STOP regression: OK")


asyncio.run(main())
