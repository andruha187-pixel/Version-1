import os
import time
import tempfile
import asyncio
import importlib.util
from pathlib import Path
import zipfile

TMP = tempfile.mkdtemp(prefix='gate64_safe_test_')
os.environ['DATA_DIR'] = TMP
os.environ['TELEGRAM_BOT_TOKEN'] = ''
os.environ['TELEGRAM_CHAT_ID'] = ''
os.environ['PAPER_START_BALANCE'] = '500'
os.environ['MIN_FREE_CASH'] = '5'
os.environ['ENTRY_ORDER_SIZE'] = '5'
os.environ['PYRAMID_ORDER_SIZE'] = '10'
os.environ['ENTRY_MOVE'] = '0.03'
os.environ['PYRAMID_STEP'] = '0.08'
os.environ['LOOKBACK_TICKS'] = '2'
os.environ['V2_ELIGIBLE_PRICE_MIN'] = '0.55'
os.environ['V2_ELIGIBLE_PRICE_MAX'] = '0.75'
os.environ['V2_ELIGIBLE_MOM_MIN'] = '0.03'
os.environ['V2_ELIGIBLE_MOM_MAX'] = '0.30'
os.environ['SAFE_ENTRY_PRICE_MIN'] = '0.64'
os.environ['SAFE_ENTRY_PRICE_MAX'] = '0.75'
os.environ['SAFE_ENTRY_MOM_MIN'] = '0.05'
os.environ['SAFE_ENTRY_MOM_MAX'] = '0.10'
os.environ['PYRAMID_MOMENTUM_CAP'] = '0.30'
os.environ['MAX_BUYS_SIDE'] = '2'

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('bot', HERE / 'main.py')
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()
bot.state_set('trading_enabled', '1')

assert bot.STRATEGY_NAME == 'M03_V2_GATE64_SAFE'
assert abs(bot.ENTRY_ORDER_SIZE - 5) < 1e-9
assert abs(bot.PYRAMID_ORDER_SIZE - 10) < 1e-9
assert bot.MAX_BUYS_SIDE == 2
assert abs(bot.paper_cash() - 500) < 1e-9

now = int(time.time())
hour_start = (now // 3600) * 3600
slot = (now // 300) * 300


def market(cid, up, down, offset=0):
    m = {
        'condition_id': cid,
        'question': 'Bitcoin Up or Down test',
        'slug': f'btc-updown-5m-{slot+offset}',
        'start_ts': slot + offset,
        'end_ts': slot + offset + 300,
        'up_asset': up,
        'down_asset': down,
    }
    bot.markets[cid] = m
    bot.persist_market(m)
    return m


def book(asset, ask, bid=None, size=100):
    if bid is None:
        bid = max(0.01, ask - 0.01)
    bot.books[asset] = {
        'bids': {float(bid): float(size)},
        'asks': {float(ask): float(size)},
        'received_ms': bot.now_ms(),
        'source': 'test',
    }


def hist(cid, asset, ref, mid, cur):
    ts = bot.now_ms()
    bot.price_history[cid][asset].clear()
    bot.price_history[cid][asset].extend([
        (ts-6000, ref),
        (ts-3000, mid),
        (ts, cur),
    ])

# ------------------------------------------------------------------
# 1) Raw M03 signal below original V2 price floor must be IGNORED,
#    not used to blacklist the market.
# ------------------------------------------------------------------
m0 = market('ignore-cheap', 'U0', 'D0')
book('U0', 0.50, 0.49); book('D0', 0.50, 0.49)
hist('ignore-cheap', 'U0', 0.44, 0.47, 0.50)  # mom .06
hist('ignore-cheap', 'D0', 0.56, 0.53, 0.50)
asyncio.run(bot.evaluate_variant(m0, bot.STRATEGY, 20.0))
with bot.db() as c:
    assert c.execute("SELECT COUNT(*) c FROM gate_decisions WHERE condition_id='ignore-cheap'").fetchone()['c'] == 0
    assert c.execute("SELECT COUNT(*) c FROM paper_trades WHERE condition_id='ignore-cheap'").fetchone()['c'] == 0

# Later first V2-eligible signal at .68 / mom .07 should still PASS.
book('U0', 0.68, 0.67); book('D0', 0.32, 0.31)
hist('ignore-cheap', 'U0', 0.61, 0.64, 0.68)
hist('ignore-cheap', 'D0', 0.39, 0.36, 0.32)
asyncio.run(bot.evaluate_variant(m0, bot.STRATEGY, 35.0))
with bot.db() as c:
    g = c.execute("SELECT * FROM gate_decisions WHERE condition_id='ignore-cheap'").fetchone()
    tr = c.execute("SELECT * FROM paper_trades WHERE condition_id='ignore-cheap'").fetchall()
assert g['passed'] == 1 and g['reason'] == 'SAFE_ENTRY_OK'
assert len(tr) == 1 and tr[0]['signal_type'] == 'ENTRY'
assert abs(tr[0]['requested_shares'] - 5.0) < 1e-9
assert abs(tr[0]['filled_shares'] - 5.0) < 1e-9

# ------------------------------------------------------------------
# 2) First V2-eligible signal at .60 / .06 must SKIP forever.
# ------------------------------------------------------------------
m1 = market('skip-price', 'U1', 'D1', 300)
book('U1', 0.60, 0.59); book('D1', 0.40, 0.39)
hist('skip-price', 'U1', 0.54, 0.57, 0.60)
hist('skip-price', 'D1', 0.46, 0.43, 0.40)
asyncio.run(bot.evaluate_variant(m1, bot.STRATEGY, 25.0))
with bot.db() as c:
    g = c.execute("SELECT * FROM gate_decisions WHERE condition_id='skip-price'").fetchone()
assert g['passed'] == 0 and g['reason'] == 'SAFE_PRICE_LOW'

# Later good-looking signal must NOT revive it.
book('U1', 0.68, 0.67); book('D1', 0.32, 0.31)
hist('skip-price', 'U1', 0.61, 0.64, 0.68)
asyncio.run(bot.evaluate_variant(m1, bot.STRATEGY, 55.0))
with bot.db() as c:
    assert c.execute("SELECT COUNT(*) c FROM paper_trades WHERE condition_id='skip-price'").fetchone()['c'] == 0

# ------------------------------------------------------------------
# 3) First V2-eligible signal with too-low momentum .04 also SKIPs.
# ------------------------------------------------------------------
m2 = market('skip-mom', 'U2', 'D2', 600)
book('U2', 0.66, 0.65); book('D2', 0.34, 0.33)
hist('skip-mom', 'U2', 0.62, 0.64, 0.66)  # mom .04
hist('skip-mom', 'D2', 0.38, 0.36, 0.34)
asyncio.run(bot.evaluate_variant(m2, bot.STRATEGY, 25.0))
with bot.db() as c:
    g = c.execute("SELECT * FROM gate_decisions WHERE condition_id='skip-mom'").fetchone()
assert g['passed'] == 0 and g['reason'] == 'SAFE_MOMENTUM_LOW'

# ------------------------------------------------------------------
# 4) Trajectory: current executable exit PnL + MFE/MAE.
# ------------------------------------------------------------------
# Existing open U0 entry is 5 shares. Provide enough bid depth.
book('U0', 0.69, 0.66, 100); book('D0', 0.32, 0.30, 100)
assert bot.record_position_trajectory(m0, 40.0)
book('U0', 0.74, 0.72, 100)
assert bot.record_position_trajectory(m0, 43.0)
book('U0', 0.62, 0.60, 100)
assert bot.record_position_trajectory(m0, 46.0)
with bot.db() as c:
    rows = c.execute("SELECT * FROM position_trajectory WHERE condition_id='ignore-cheap' ORDER BY id").fetchall()
assert len(rows) == 3
assert all(r['unrealized_pnl'] is not None for r in rows)
assert rows[-1]['mfe_pnl'] >= rows[0]['unrealized_pnl']
assert rows[-1]['mae_pnl'] <= rows[0]['unrealized_pnl']
assert abs(rows[-1]['exit_filled_shares'] - 5.0) < 1e-9

# ------------------------------------------------------------------
# 5) One 10-share pyramid at +.08 from .68; then hard cap at 2 buys.
# ------------------------------------------------------------------
book('U0', 0.76, 0.75, 100)
hist('ignore-cheap', 'U0', 0.71, 0.73, 0.76)  # positive mom .05
asyncio.run(bot.evaluate_variant(m0, bot.STRATEGY, 70.0))
with bot.db() as c:
    tr = c.execute("SELECT * FROM paper_trades WHERE condition_id='ignore-cheap' ORDER BY id").fetchall()
assert len(tr) == 2
assert tr[1]['signal_type'] == 'PYRAMID'
assert abs(tr[1]['requested_shares'] - 10.0) < 1e-9
assert abs(sum(float(x['filled_shares']) for x in tr) - 15.0) < 1e-9

book('U0', 0.86, 0.85, 100)
hist('ignore-cheap', 'U0', 0.80, 0.83, 0.86)
asyncio.run(bot.evaluate_variant(m0, bot.STRATEGY, 100.0))
with bot.db() as c:
    assert c.execute("SELECT COUNT(*) c FROM paper_trades WHERE condition_id='ignore-cheap'").fetchone()['c'] == 2

# Settlement cash accounting.
asyncio.run(bot.settle_market('ignore-cheap', 'U0', 'Up'))
with bot.db() as c:
    result = c.execute("SELECT * FROM market_results WHERE condition_id='ignore-cheap'").fetchone()
assert result is not None and result['trades'] == 2
assert abs(result['payout'] - 15.0) < 1e-9
assert abs(bot.paper_cash() - (500.0 + float(result['pnl']))) < 1e-6

# Hourly report keeps the old files and adds trajectory.
path, summary = bot.make_report(hour_start, hour_start + 3600)
assert path.exists()
with zipfile.ZipFile(path, 'r') as z:
    names = set(z.namelist())
for expected in {
    'strategy_summary.csv', 'variants_summary.csv', 'gate_decisions.csv',
    'paper_trades.csv', 'signals.csv', 'market_results.csv', 'markets.csv',
    'position_trajectory.csv', 'report.txt'
}:
    assert expected in names, expected

print('GATE64 SAFE regression: OK')
