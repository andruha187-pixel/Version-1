import os
import time
import asyncio
import tempfile
import importlib.util
import zipfile
from pathlib import Path

tmp = tempfile.mkdtemp(prefix="multi7_fghj_")
os.environ["DATA_DIR"] = tmp
os.environ["TELEGRAM_BOT_TOKEN"] = ""
os.environ["TELEGRAM_CHAT_ID"] = ""
os.environ["SYMBOLS"] = "BTC,XRP,BNB,SOL,ETH,DOGE,HYPE"
os.environ["PAPER_START_BALANCE"] = "500"

here = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("bot", here/"main.py")
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

SYMS = ["BTC","XRP","BNB","SOL","ETH","DOGE","HYPE"]

# ------------------------------------------------------------
# Configuration / 28 independent accounts
# ------------------------------------------------------------
assert bot.SYMBOLS == SYMS
assert len(bot.STRATEGIES) == 28
for symbol in SYMS:
    F,G,H,J = bot.STRATEGIES_BY_SYMBOL[symbol]
    assert [v["code"] for v in (F,G,H,J)] == ["F","G","H","J"]
    assert len({v["name"] for v in (F,G,H,J)}) == 4
    assert all(bot.paper_cash(v["name"]) == 500 for v in (F,G,H,J))
    assert all(v["stop_loss_price"] is None for v in (F,G,H,J))

    assert (F["safe_entry_price_min"],F["safe_entry_price_max"]) == (0.67,0.70)
    assert (G["safe_entry_price_min"],G["safe_entry_price_max"]) == (0.67,0.70)
    assert (H["safe_entry_price_min"],H["safe_entry_price_max"]) == (0.67,0.70)
    assert (J["safe_entry_price_min"],J["safe_entry_price_max"]) == (0.67,0.75)
    assert F["consensus_min_other_tokens"] == 1
    assert G["consensus_min_other_tokens"] == 2
    assert H["consensus_min_other_tokens"] == 2
    assert J["consensus_min_other_tokens"] == 2
    assert not F["dca_enabled"] and not G["dca_enabled"] and H["dca_enabled"] and not J["dca_enabled"]

assert bot.SAFE_ENTRY_MOM_MIN == 0.05
assert bot.SAFE_ENTRY_MOM_MAX == 0.10
assert bot.V2_ELIGIBLE_PRICE_MIN == 0.55
assert bot.V2_ELIGIBLE_PRICE_MAX == 0.75
assert bot.V2_ELIGIBLE_MOM_MIN == 0.03
assert bot.V2_ELIGIBLE_MOM_MAX == 0.30

H0 = bot.STRATEGIES_BY_SYMBOL["BTC"][2]
assert H0["dca_arm_price"] == 0.50
assert H0["dca_min_buy_price"] == 0.30
assert H0["dca_max_buy_price"] == 0.60
assert H0["dca_rebound_mom"] == 0.05
assert H0["dca_rebound_mom_max"] == 0.15
assert H0["dca_deadline_sec"] == 120

source = (here/"main.py").read_text(encoding="utf-8")
assert "async def stop_loss_loop" not in source
assert "STOP_LOSS_PRICE" not in source


def fresh_book(asset, bid, ask, size=100.0):
    bot.books[asset] = {
        "bids": {float(bid): float(size)},
        "asks": {float(ask): float(size)},
        "received_ms": bot.now_ms(),
        "source": "test",
    }


slot = (int(time.time()) // 300) * 300
counter = 0

def make_market(symbol, tag):
    global counter
    counter += 1
    cfg = bot.ASSET_CONFIG[symbol]
    s = slot
    m = {
        "condition_id": f"cid-{symbol}-{tag}-{counter}",
        "symbol": symbol,
        "question": f"{cfg['label']} Up or Down Test",
        "slug": f"{cfg['prefix']}-{s}",
        "start_ts": s,
        "end_ts": s + 300,
        "up_asset": f"{symbol}UP-{tag}-{counter}",
        "down_asset": f"{symbol}DN-{tag}-{counter}",
    }
    bot.markets[m["condition_id"]] = m
    bot.persist_market(m)
    return m


def seed_up(m, ask, mom):
    ms = bot.now_ms()
    ref = ask - mom
    mid = ref + mom/2
    fresh_book(m["up_asset"], max(.01,ask-.01), ask)
    fresh_book(m["down_asset"], max(.01,1-ask-.01), max(.01,1-ask))
    h = bot.price_history[m["condition_id"]][m["up_asset"]]
    h.clear()
    h.extend([(ms-6000,ref),(ms-3000,mid),(ms,ask)])
    hd = bot.price_history[m["condition_id"]][m["down_asset"]]
    hd.clear()
    hd.extend([(ms-6000,.45),(ms-3000,.40),(ms,.35)])


def set_up_path(m, ref, mid, ask):
    ms = bot.now_ms()
    fresh_book(m["up_asset"], max(.01,ask-.01), ask)
    h = bot.price_history[m["condition_id"]][m["up_asset"]]
    h.clear()
    h.extend([(ms-6000,ref),(ms-3000,mid),(ms,ask)])


def record_vote(m, ask, mom, decision_ms):
    seed_up(m, ask, mom)
    bot.record_first_v2_vote(m, 30.0, decision_ms)
    row = bot.first_v2_vote(m["condition_id"])
    assert row is not None
    assert row["outcome"] == "Up"
    return row


# ------------------------------------------------------------
# Critical property: confirmations are FIRST V2-eligible votes,
# NOT SAFE67 passes.  ETH/SOL votes below SAFE entry still count.
# ------------------------------------------------------------
base_ms = bot.now_ms()
m_eth = make_market("ETH","v2-src")
m_sol = make_market("SOL","v2-src")
record_vote(m_eth, .60, .04, base_ms-7000)  # V2-eligible, not SAFE target
record_vote(m_sol, .61, .04, base_ms-3000)  # V2-eligible, not SAFE target

m_btc = make_market("BTC","target")
record_vote(m_btc, .69, .07, base_ms)

F,G,H,J = bot.STRATEGIES_BY_SYMBOL["BTC"]
for v in (F,G,H,J):
    asyncio.run(bot.evaluate_consensus_variant(m_btc, v, 35.0))
    assert bot.position_totals(m_btc["condition_id"], v["name"])["bought"] == 5

with bot.db() as conn:
    rows = conn.execute(
        "SELECT variant,confirm_count,confirm_symbols_json,passed FROM consensus_events WHERE condition_id=?",
        (m_btc["condition_id"],)
    ).fetchall()
assert len(rows) == 4
for r in rows:
    assert r["confirm_count"] == 2
    assert r["passed"] == 1
    syms = set(bot.parse_jsonish(r["confirm_symbols_json"]))
    assert syms == {"ETH","SOL"}

# Source first-V2 rows exist even though .60/.61 cannot satisfy SAFE67 target gate.
assert bot.first_v2_vote(m_eth["condition_id"])["ask"] == .60
assert bot.first_v2_vote(m_sol["condition_id"])["ask"] == .61

# ------------------------------------------------------------
# Exactly one recent other-token vote:
# F passes; G/H/J must skip permanently.
# ------------------------------------------------------------
with bot.db() as conn:
    conn.execute("UPDATE v2_votes SET decision_ms=?", (base_ms-30000,))
    conn.commit()

m_doge = make_market("DOGE","one-src")
record_vote(m_doge, .60, .04, base_ms+1000)

m_xrp = make_market("XRP","one-target")
record_vote(m_xrp, .69, .07, base_ms+2000)
FX,GX,HX,JX = bot.STRATEGIES_BY_SYMBOL["XRP"]

for v in (FX,GX,HX,JX):
    asyncio.run(bot.evaluate_consensus_variant(m_xrp, v, 35.0))

assert bot.position_totals(m_xrp["condition_id"], FX["name"])["bought"] == 5
assert bot.position_totals(m_xrp["condition_id"], GX["name"])["bought"] == 0
assert bot.position_totals(m_xrp["condition_id"], HX["name"])["bought"] == 0
assert bot.position_totals(m_xrp["condition_id"], JX["name"])["bought"] == 0

with bot.db() as conn:
    frow = conn.execute(
        "SELECT * FROM consensus_events WHERE condition_id=? AND variant=?",
        (m_xrp["condition_id"],FX["name"])
    ).fetchone()
    grow = conn.execute(
        "SELECT * FROM consensus_events WHERE condition_id=? AND variant=?",
        (m_xrp["condition_id"],GX["name"])
    ).fetchone()
assert frow["passed"] == 1 and frow["confirm_count"] == 1
assert grow["passed"] == 0 and grow["reason"] == "V2_CONSENSUS_INSUFFICIENT"

# ------------------------------------------------------------
# Target .72: tight F/G/H reject, wide J accepts with 2 V2 votes.
# ------------------------------------------------------------
with bot.db() as conn:
    conn.execute("UPDATE v2_votes SET decision_ms=?", (base_ms-30000,))
    conn.commit()

m_bnb1 = make_market("BNB","src1")
m_sol2 = make_market("SOL","src2")
record_vote(m_bnb1,.60,.04,base_ms+3000)
record_vote(m_sol2,.62,.04,base_ms+4000)

m_eth72 = make_market("ETH","wide-target")
record_vote(m_eth72,.72,.07,base_ms+5000)
FE,GE,HE,JE = bot.STRATEGIES_BY_SYMBOL["ETH"]
for v in (FE,GE,HE,JE):
    asyncio.run(bot.evaluate_consensus_variant(m_eth72,v,35.0))

assert bot.position_totals(m_eth72["condition_id"],FE["name"])["bought"] == 0
assert bot.position_totals(m_eth72["condition_id"],GE["name"])["bought"] == 0
assert bot.position_totals(m_eth72["condition_id"],HE["name"])["bought"] == 0
assert bot.position_totals(m_eth72["condition_id"],JE["name"])["bought"] == 5

# ------------------------------------------------------------
# H safer DCA: arm only; reject <.30; reject momentum >+.15;
# then valid .30-.60 and +.05..+.15 -> exactly one DCA.
# BTC H already has its 5-share entry from first scenario.
# ------------------------------------------------------------
set_up_path(m_btc,.58,.54,.50)
asyncio.run(bot.evaluate_consensus_variant(m_btc,H,60.0))
assert bot.get_variant_state(m_btc["condition_id"],H)["dca_armed"]
assert bot.position_totals(m_btc["condition_id"],H["name"])["bought"] == 5

set_up_path(m_btc,.19,.22,.25)  # +.06 but below .30
asyncio.run(bot.evaluate_consensus_variant(m_btc,H,70.0))
assert bot.position_totals(m_btc["condition_id"],H["name"])["bought"] == 5

set_up_path(m_btc,.15,.25,.35)  # +.20, too sharp
asyncio.run(bot.evaluate_consensus_variant(m_btc,H,80.0))
assert bot.position_totals(m_btc["condition_id"],H["name"])["bought"] == 5

set_up_path(m_btc,.25,.30,.35)  # +.10, valid
asyncio.run(bot.evaluate_consensus_variant(m_btc,H,90.0))
ph = bot.position_totals(m_btc["condition_id"],H["name"])
assert ph["bought"] == 10 and ph["dca_trades"] == 1

# F/G/J never DCA.
for v in (F,G,J):
    set_up_path(m_btc,.25,.30,.35)
    asyncio.run(bot.evaluate_consensus_variant(m_btc,v,90.0))
    assert bot.position_totals(m_btc["condition_id"],v["name"])["bought"] == 5

# ------------------------------------------------------------
# Settlement stays symbol-scoped: exactly BTC F/G/H/J results.
# ------------------------------------------------------------
asyncio.run(bot.settle_market(m_btc["condition_id"],m_btc["up_asset"],"Up"))
with bot.db() as conn:
    rr = conn.execute(
        "SELECT variant FROM market_results WHERE condition_id=?",
        (m_btc["condition_id"],)
    ).fetchall()
assert {r["variant"] for r in rr} == {
    "BTC_F_TIGHT_ONE_V2",
    "BTC_G_TIGHT_TWO_V2",
    "BTC_H_TIGHT_TWO_V2_SAFE_DCA",
    "BTC_J_WIDE_TWO_V2",
}

# ------------------------------------------------------------
# Hourly ZIP structure
# ------------------------------------------------------------
hour_start = (int(time.time())//3600)*3600
path,summaries = bot.make_report(hour_start,hour_start+3600)
assert len(summaries) == 28
with zipfile.ZipFile(path,"r") as z:
    names=set(z.namelist())

required={"variants_summary.csv","markets.csv","v2_votes.csv","report.txt"}
folders={
    "F":"F_tight67_70_one_v2_5sh",
    "G":"G_tight67_70_two_v2_5sh",
    "H":"H_tight67_70_two_v2_safe_dca_5plus5",
    "J":"J_wide67_75_two_v2_5sh",
}
for symbol in SYMS:
    for code,folder in folders.items():
        base=f"{symbol}/{folder}"
        required.update({
            f"{base}/summary.csv",
            f"{base}/gate_decisions.csv",
            f"{base}/paper_trades.csv",
            f"{base}/dca_events.csv",
            f"{base}/consensus_events.csv",
            f"{base}/market_results.csv",
            f"{base}/position_trajectory.csv",
        })
assert required.issubset(names), required-names

print("MULTI7 FIRST-V2 CONSENSUS F/G/H/J regression: OK")
