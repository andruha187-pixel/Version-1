import os, tempfile, importlib.util, asyncio, math
from pathlib import Path

TMP = tempfile.mkdtemp(prefix='multi7_be_test_')
os.environ['DATA_DIR'] = TMP
os.environ['BREAKEVEN_STOP_ENABLED'] = '1'
os.environ['BREAKEVEN_TRIGGER_MOVE'] = '0.05'
os.environ['BREAKEVEN_MIN_PROFIT_USDC'] = '0.10'

path = Path(__file__).with_name('main.py')
spec = importlib.util.spec_from_file_location('multi7_be', path)
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()


def set_book(asset, ask=None, bid=None, qty=100.0):
    bot.books[asset] = {
        'asks': ({float(ask): qty} if ask is not None else {}),
        'bids': ({float(bid): qty} if bid is not None else {}),
        'received_ms': bot.now_ms(),
        'source': 'test',
    }


async def main():
    v = next(x for x in bot.STRATEGIES if x['symbol'] == 'BTC' and x['code'] == 'F')
    cid = 'be-test-1'
    market = {
        'condition_id': cid, 'symbol': 'BTC', 'question': 'test', 'slug': 'btc-updown-5m-0',
        'start_ts': bot.now_ts() - 30, 'end_ts': bot.now_ts() + 270,
        'up_asset': 'UP1', 'down_asset': 'DN1',
    }
    bot.markets[cid] = market
    bot.persist_market(market)

    # ENTRY 5 shares at 0.68. Entry fee is included in buy_cost.
    set_book('UP1', ask=0.68, bid=0.67)
    set_book('DN1', ask=0.32, bid=0.31)
    assert await bot.execute_paper(cid, v, 'UP1', 'Up', 'ENTRY')
    pos = bot.position_totals(cid, v['name'])
    assert abs(bot.weighted_gross_entry_avg(pos) - 0.68) < 1e-9
    be = bot.fee_adjusted_profit_stop_price(pos)
    assert be is not None and 0.68 < be < 0.73, be

    # Below +0.05: no arm.
    set_book('UP1', ask=0.73, bid=0.729)
    await bot.process_breakeven_stop(market, v, 31.0)
    assert bot.breakeven_event(cid, v['name']) is None

    # At +0.05 executable BID: arm, but never stop on the arming tick.
    set_book('UP1', ask=0.74, bid=0.73)
    await bot.process_breakeven_stop(market, v, 32.0)
    ev = bot.breakeven_event(cid, v['name'])
    assert ev is not None and ev['triggered_ms'] is None
    assert bot.position_totals(cid, v['name'])['remaining'] > 4.999

    # Global STOP must not disable protection.
    bot.state_set('trading_enabled', '0')

    # Stay above BE => hold.
    set_book('UP1', ask=be + 0.02, bid=be + 0.01)
    await bot.process_breakeven_stop(market, v, 33.0)
    assert bot.position_totals(cid, v['name'])['remaining'] > 4.999

    # Fall to the calculated fee-adjusted BE => exit.
    set_book('UP1', ask=be + 0.01, bid=be, qty=100)
    await bot.process_breakeven_stop(market, v, 34.0)
    pos2 = bot.position_totals(cid, v['name'])
    assert pos2['remaining'] <= 1e-8, pos2['remaining']
    assert bot.stop_triggered(cid, v['name'])
    pnl = pos2['exit_net'] - pos2['buy_cost']
    assert pnl >= 0.0999, pnl
    assert pnl <= 0.1001, pnl

    # Stop exit is visible in DB.
    with bot.db() as conn:
        r = conn.execute("SELECT reason FROM paper_exits WHERE condition_id=? AND variant=?", (cid, v['name'])).fetchone()
        assert r and r['reason'] == 'BREAKEVEN_STOP'

    # H: if DCA happens before BE arm, weighted average and fee-adjusted stop use both buys.
    h = next(x for x in bot.STRATEGIES if x['symbol'] == 'ETH' and x['code'] == 'H')
    cid2 = 'be-test-h'
    m2 = {
        'condition_id': cid2, 'symbol': 'ETH', 'question': 'test h', 'slug': 'eth-updown-5m-0',
        'start_ts': bot.now_ts() - 40, 'end_ts': bot.now_ts() + 260,
        'up_asset': 'UP2', 'down_asset': 'DN2',
    }
    bot.markets[cid2] = m2
    bot.persist_market(m2)
    set_book('UP2', ask=0.68, bid=0.67)
    set_book('DN2', ask=0.32, bid=0.31)
    assert await bot.execute_paper(cid2, h, 'UP2', 'Up', 'ENTRY')
    set_book('UP2', ask=0.40, bid=0.39)
    assert await bot.execute_paper(cid2, h, 'UP2', 'Up', 'DCA')
    ph = bot.position_totals(cid2, h['name'])
    avg = bot.weighted_gross_entry_avg(ph)
    assert abs(avg - 0.54) < 1e-9, avg
    beh = bot.fee_adjusted_profit_stop_price(ph)
    assert beh is not None and beh > avg
    set_book('UP2', ask=0.60, bid=0.59)
    await bot.process_breakeven_stop(m2, h, 45.0)
    evh = bot.breakeven_event(cid2, h['name'])
    assert evh is not None, 'H should arm at avg 0.54 + 0.05 = 0.59'
    assert abs(float(evh['arm_trigger_price']) - 0.59) < 1e-9

    # Rare partial fill: +0.05 may not be enough dollars to protect +$0.10.
    # The effective arm must then wait until the profit-stop price is reached.
    pv = next(x for x in bot.STRATEGIES if x['symbol'] == 'XRP' and x['code'] == 'F')
    cid3 = 'profit-partial-fill'
    m3 = {
        'condition_id': cid3, 'symbol': 'XRP', 'question': 'partial', 'slug': 'xrp-updown-5m-0',
        'start_ts': bot.now_ts() - 20, 'end_ts': bot.now_ts() + 280,
        'up_asset': 'UP3', 'down_asset': 'DN3',
    }
    bot.markets[cid3] = m3
    bot.persist_market(m3)
    set_book('UP3', ask=0.68, bid=0.67, qty=1.0)
    set_book('DN3', ask=0.32, bid=0.31, qty=100.0)
    assert await bot.execute_paper(cid3, pv, 'UP3', 'Up', 'ENTRY')
    pp = bot.position_totals(cid3, pv['name'])
    assert 0.99 < pp['remaining'] < 1.01
    pstop = bot.fee_adjusted_profit_stop_price(pp)
    assert pstop is not None and pstop > 0.73, pstop
    set_book('UP3', ask=0.731, bid=0.73, qty=100.0)
    await bot.process_breakeven_stop(m3, pv, 21.0)
    assert bot.breakeven_event(cid3, pv['name']) is None
    set_book('UP3', ask=pstop + 0.001, bid=pstop, qty=100.0)
    await bot.process_breakeven_stop(m3, pv, 22.0)
    assert bot.breakeven_event(cid3, pv['name']) is not None

    # No runtime hourly reporter remains.
    assert not hasattr(bot, 'report_loop')

    print('MULTI7 F/G/H/J fee-adjusted +$0.10 profit-stop regression: OK')
    print(f'Example 0.68 entry -> arm 0.7300 -> calculated +$0.10 stop {be:.6f} -> realized PnL {pnl:+.6f}')
    print(f'H 0.68 + 0.40 equal-size buys -> weighted gross avg {avg:.4f}, arm {avg+0.05:.4f}, +$0.10 stop {beh:.6f}')

asyncio.run(main())
