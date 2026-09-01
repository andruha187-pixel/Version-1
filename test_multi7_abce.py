import os
import time
import asyncio
import tempfile
import importlib.util
import zipfile
from pathlib import Path

tmp = tempfile.mkdtemp(prefix="safe67_multi7_abce_")
os.environ["DATA_DIR"] = tmp
os.environ["TELEGRAM_BOT_TOKEN"] = ""
os.environ["TELEGRAM_CHAT_ID"] = ""
os.environ["SYMBOLS"] = "BTC,XRP,BNB,SOL,ETH,DOGE,HYPE"
os.environ["PAPER_START_BALANCE"] = "500"

here = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("bot", here / "main.py")
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

SYMS = ["BTC","XRP","BNB","SOL","ETH","DOGE","HYPE"]

# ------------------------------------------------------------------
# Configuration / account isolation
# ------------------------------------------------------------------
assert bot.SYMBOLS == SYMS
assert len(bot.STRATEGIES) == 28
for symbol in SYMS:
    variants = bot.STRATEGIES_BY_SYMBOL[symbol]
    assert [v["code"] for v in variants] == ["A","B","C","E"]
    assert len({v["name"] for v in variants}) == 4
    for v in variants:
        assert bot.paper_cash(v["name"]) == 500
        assert v["stop_loss_price"] is None

    A,B,C,E = variants
    assert (A["safe_entry_price_min"], A["safe_entry_price_max"]) == (0.67,0.75)
    assert (B["safe_entry_price_min"], B["safe_entry_price_max"]) == (0.67,0.75)
    assert (C["safe_entry_price_min"], C["safe_entry_price_max"]) == (0.67,0.70)
    assert (E["safe_entry_price_min"], E["safe_entry_price_max"]) == (0.67,0.75)
    assert not A["dca_enabled"]
    assert B["dca_enabled"]
    assert C["dca_enabled"]
    assert not E["dca_enabled"] and E["consensus_enabled"]
    assert C["dca_min_buy_price"] == 0.30
    assert C["dca_max_buy_price"] == 0.60
    assert C["dca_rebound_mom"] == 0.05
    assert C["dca_rebound_mom_max"] == 0.15
    assert E["consensus_min_other_tokens"] == 2
    assert E["consensus_window_sec"] == 10

assert bot.SAFE_ENTRY_MOM_MIN == 0.05
assert bot.SAFE_ENTRY_MOM_MAX == 0.10
assert bot.DCA_ARM_PRICE == 0.50
assert bot.DCA_DEADLINE_SEC == 120

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

def make_market(symbol, suffix, offset=0):
    cfg = bot.ASSET_CONFIG[symbol]
    s = slot + offset
    m = {
        "condition_id": f"cid-{symbol}-{suffix}",
        "symbol": symbol,
        "question": f"{cfg['label']} Up or Down Test",
        "slug": f"{cfg['prefix']}-{s}",
        "start_ts": s,
        "end_ts": s + 300,
        "up_asset": f"{symbol}UP-{suffix}",
        "down_asset": f"{symbol}DN-{suffix}",
    }
    bot.markets[m["condition_id"]] = m
    bot.persist_market(m)
    return m


def seed_up_entry(m, ask=.68, mom=.07):
    # exact 2-tick momentum: current-ref = mom
    ms = bot.now_ms()
    ref = ask - mom
    mid = (ref + ask) / 2
    fresh_book(m["up_asset"], max(.01, ask-.01), ask)
    fresh_book(m["down_asset"], max(.01, 1-ask-.01), max(.01, 1-ask))
    h = bot.price_history[m["condition_id"]][m["up_asset"]]
    h.clear()
    h.extend([(ms-6000, ref), (ms-3000, mid), (ms, ask)])
    hd = bot.price_history[m["condition_id"]][m["down_asset"]]
    hd.clear()
    hd.extend([(ms-6000, .40), (ms-3000, .36), (ms, .32)])


def set_up_path(m, ref, mid, ask):
    ms = bot.now_ms()
    fresh_book(m["up_asset"], max(.01, ask-.01), ask)
    h = bot.price_history[m["condition_id"]][m["up_asset"]]
    h.clear()
    h.extend([(ms-6000, ref), (ms-3000, mid), (ms, ask)])


# ------------------------------------------------------------------
# A/B unchanged behavior on BTC.
# ------------------------------------------------------------------
m_ab = make_market("BTC", "ab")
A,B,C,E = bot.STRATEGIES_BY_SYMBOL["BTC"]
seed_up_entry(m_ab, .68, .07)
asyncio.run(bot.evaluate_variant(m_ab, A, 30.0))
asyncio.run(bot.evaluate_variant(m_ab, B, 30.0))
assert bot.position_totals(m_ab["condition_id"], A["name"])["bought"] == 5
assert bot.position_totals(m_ab["condition_id"], B["name"])["bought"] == 5

# A never adds.
set_up_path(m_ab, .68, .72, .76)
asyncio.run(bot.evaluate_variant(m_ab, A, 50.0))
assert bot.position_totals(m_ab["condition_id"], A["name"])["bought"] == 5

# B: arm at .50, then old behavior still permits a deep .25 rebound DCA.
set_up_path(m_ab, .58, .54, .50)
asyncio.run(bot.evaluate_variant(m_ab, B, 70.0))
assert bot.get_variant_state(m_ab["condition_id"], B)["dca_armed"]
assert bot.position_totals(m_ab["condition_id"], B["name"])["bought"] == 5
set_up_path(m_ab, .19, .22, .25)  # +.06 over two ticks
asyncio.run(bot.evaluate_variant(m_ab, B, 80.0))
assert bot.position_totals(m_ab["condition_id"], B["name"])["bought"] == 10

# ------------------------------------------------------------------
# C exact agreed rules.
# ------------------------------------------------------------------
m_c = make_market("ETH", "c")
C = bot.STRATEGIES_BY_SYMBOL["ETH"][2]
seed_up_entry(m_c, .69, .07)
asyncio.run(bot.evaluate_variant(m_c, C, 30.0))
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 5

# Arm only, no buy.
set_up_path(m_c, .58, .54, .50)
asyncio.run(bot.evaluate_variant(m_c, C, 60.0))
assert bot.get_variant_state(m_c["condition_id"], C)["dca_armed"]
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 5

# Rebound below .30 is rejected.
set_up_path(m_c, .19, .22, .25)  # +.06
asyncio.run(bot.evaluate_variant(m_c, C, 70.0))
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 5

# Rebound momentum above +.15 is rejected.
set_up_path(m_c, .15, .25, .35)  # +.20
asyncio.run(bot.evaluate_variant(m_c, C, 80.0))
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 5

# Valid .30-.60 price and +.05..+.15 momentum -> exactly one DCA.
set_up_path(m_c, .25, .30, .35)  # +.10
asyncio.run(bot.evaluate_variant(m_c, C, 90.0))
pc = bot.position_totals(m_c["condition_id"], C["name"])
assert pc["bought"] == 10 and pc["dca_trades"] == 1

# C entry > .70 permanently skips.
m_c_hi = make_market("BNB", "c-hi")
C_bnb = bot.STRATEGIES_BY_SYMBOL["BNB"][2]
seed_up_entry(m_c_hi, .72, .07)
asyncio.run(bot.evaluate_variant(m_c_hi, C_bnb, 30.0))
with bot.db() as conn:
    g = conn.execute(
        "SELECT * FROM gate_decisions WHERE condition_id=? AND variant=?",
        (m_c_hi["condition_id"], C_bnb["name"])
    ).fetchone()
assert g and g["passed"] == 0 and g["reason"] == "SAFE_PRICE_HIGH"
assert bot.position_totals(m_c_hi["condition_id"], C_bnb["name"])["bought"] == 0

# ------------------------------------------------------------------
# E cross-token consensus.
# ETH A and SOL A vote Up, then BTC E gets 2 confirmations.
# ------------------------------------------------------------------
for symbol, suffix in [("ETH","vote"),("SOL","vote")]:
    m = make_market(symbol, suffix)
    A_src = bot.STRATEGIES_BY_SYMBOL[symbol][0]
    seed_up_entry(m, .68, .07)
    asyncio.run(bot.evaluate_variant(m, A_src, 30.0))
    assert bot.position_totals(m["condition_id"], A_src["name"])["bought"] == 5

m_e = make_market("BTC", "consensus")
E_btc = bot.STRATEGIES_BY_SYMBOL["BTC"][3]
seed_up_entry(m_e, .68, .07)
asyncio.run(bot.evaluate_consensus_variant(m_e, E_btc, 35.0))
pe = bot.position_totals(m_e["condition_id"], E_btc["name"])
assert pe["bought"] == 5

with bot.db() as conn:
    ce = conn.execute(
        "SELECT * FROM consensus_events WHERE condition_id=? AND variant=?",
        (m_e["condition_id"], E_btc["name"])
    ).fetchone()
assert ce and ce["passed"] == 1
symbols = set(bot.parse_jsonish(ce["confirm_symbols_json"]))
assert {"ETH","SOL"}.issubset(symbols)
assert ce["confirm_count"] >= 2

# Make all prior A votes stale, then create only one fresh Up vote.
with bot.db() as conn:
    conn.execute(
        "UPDATE gate_decisions SET decision_ms=? WHERE variant LIKE '%_A_SAFE67_BASE' AND passed=1",
        (bot.now_ms()-20000,)
    )
    conn.commit()

m_one = make_market("DOGE", "one-vote")
A_doge = bot.STRATEGIES_BY_SYMBOL["DOGE"][0]
seed_up_entry(m_one, .68, .07)
asyncio.run(bot.evaluate_variant(m_one, A_doge, 30.0))

m_e_fail = make_market("XRP", "cons-fail")
E_xrp = bot.STRATEGIES_BY_SYMBOL["XRP"][3]
seed_up_entry(m_e_fail, .68, .07)
asyncio.run(bot.evaluate_consensus_variant(m_e_fail, E_xrp, 35.0))
assert bot.position_totals(m_e_fail["condition_id"], E_xrp["name"])["bought"] == 0
with bot.db() as conn:
    ce2 = conn.execute(
        "SELECT * FROM consensus_events WHERE condition_id=? AND variant=?",
        (m_e_fail["condition_id"], E_xrp["name"])
    ).fetchone()
assert ce2 and ce2["passed"] == 0
assert ce2["reason"] == "CONSENSUS_INSUFFICIENT"
assert ce2["confirm_count"] == 1

# ------------------------------------------------------------------
# Settlement is token-scoped: 4 BTC results only.
# ------------------------------------------------------------------
asyncio.run(bot.settle_market(m_e["condition_id"], m_e["up_asset"], "Up"))
with bot.db() as conn:
    rows = conn.execute(
        "SELECT variant FROM market_results WHERE condition_id=?",
        (m_e["condition_id"],)
    ).fetchall()
assert {r["variant"] for r in rows} == {
    "BTC_A_SAFE67_BASE",
    "BTC_B_SAFE67_REVERSAL_DCA",
    "BTC_C_SAFE67_TIGHT_DCA",
    "BTC_E_SAFE67_CONSENSUS",
}

# ------------------------------------------------------------------
# Hourly ZIP: 28 summaries and A/B/C/E folder for every token.
# ------------------------------------------------------------------
hour_start = slot - (slot % 3600)
path, summaries = bot.make_report(hour_start, hour_start + 3600)
assert len(summaries) == 28

with zipfile.ZipFile(path, "r") as z:
    names = set(z.namelist())

required = {"variants_summary.csv", "markets.csv", "report.txt"}
folders = {
    "A": "A_safe67_base_5sh",
    "B": "B_safe67_reversal_dca_5plus5",
    "C": "C_tight67_70_safer_dca_5plus5",
    "E": "E_safe67_consensus_5sh",
}
for symbol in SYMS:
    for code, folder in folders.items():
        base = f"{symbol}/{folder}"
        required.update({
            f"{base}/summary.csv",
            f"{base}/paper_trades.csv",
            f"{base}/dca_events.csv",
            f"{base}/consensus_events.csv",
            f"{base}/market_results.csv",
            f"{base}/position_trajectory.csv",
        })
assert required.issubset(names), required - names

print("MULTI7 SAFE67 A/B/C/E regression: OK")
