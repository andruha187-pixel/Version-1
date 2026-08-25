import math

from main import Database, FillResult, SellFillResult, WeatherPaperBot, parse_jsonish, yes_token_from_market


def test_yes_token_parsing():
    market = {
        "outcomes": '["Yes", "No"]',
        "clobTokenIds": '["YES123", "NO456"]',
    }
    assert yes_token_from_market(market) == ("YES123", "NO456")


def test_market_buy_sweeps_asks():
    asks = [
        {"price": 0.80, "size": 2.0},  # $1.60
        {"price": 0.81, "size": 10.0},
    ]
    fill = WeatherPaperBot.simulate_market_buy(asks, 5.0)
    assert math.isclose(fill.filled_notional, 5.0, rel_tol=1e-9)
    assert len(fill.levels) == 2
    assert fill.avg_price > 0.80
    assert fill.avg_price < 0.81 + 1e-9


def test_weather_fee_formula():
    fill = FillResult(
        requested_notional=5.0,
        filled_notional=5.0,
        shares=5.0 / 0.80,
        avg_price=0.80,
        levels=[{"price": 0.80, "shares": 5.0 / 0.80, "notional": 5.0}],
    )
    # C * rate * p * (1-p)
    expected = round((5.0 / 0.80) * 0.05 * 0.80 * 0.20, 5)
    assert WeatherPaperBot.taker_fee(fill, 0.05) == expected


def test_market_sell_sweeps_bids():
    bids = [
        {"price": 0.40, "size": 2.0},
        {"price": 0.39, "size": 10.0},
    ]
    sell = WeatherPaperBot.simulate_market_sell(bids, 5.0)
    assert math.isclose(sell.sold_shares, 5.0, rel_tol=1e-9)
    assert len(sell.levels) == 2
    assert sell.avg_price < 0.40
    assert sell.avg_price > 0.39 - 1e-9


def test_stop_loss_closes_and_returns_cash(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    trade_id = db.insert_trade({
        "strategy": "T79", "threshold": 0.79, "market_id": "m1", "event_id": "e1",
        "event_title": "Highest temperature in Test", "event_slug": "test",
        "temperature_label": "25C", "yes_token_id": "YES1",
        "signal_time": "2026-08-25T00:00:00Z", "fill_time": "2026-08-25T00:00:01Z",
        "previous_ask": 0.78, "trigger_ask": 0.79, "requested_notional": 5.0,
        "filled_notional": 5.0, "taker_fee": 0.01, "cash_debit": 5.01,
        "shares": 5.0 / 0.79, "avg_fill_price": 0.79,
        "best_bid_at_fill": 0.78, "best_ask_at_fill": 0.79, "fill_levels_json": "[]",
    })
    shares = 5.0 / 0.79
    sell = SellFillResult(
        requested_shares=shares, sold_shares=shares, gross_proceeds=shares * 0.40,
        avg_price=0.40, levels=[{"price": 0.40, "shares": shares, "notional": shares * 0.40}],
    )
    closed = db.close_stop_loss(trade_id, 0.40, sell, 0.01)
    assert closed is not None
    assert closed["status"] == "STOP_LOSS"
    assert closed["pnl"] < 0
    assert not db.open_trades("T79")
    assert db.free_cash("T79") < 1000.0


def _minimal_market(MarketInfo, *, market_id='m_reg', token='YES_REG'):
    return MarketInfo(
        market_id=market_id,
        event_id='e_reg',
        condition_id='0xabc',
        event_slug='highest-temperature-in-denver-on-august-24-2026',
        event_title='Highest temperature in Denver on August 24?',
        market_slug='highest-temperature-in-denver-on-august-24-2026-98forhigher',
        question='Will the highest temperature in Denver be 98°F or higher on August 24?',
        temperature_label='98°F or higher',
        end_date='2026-08-24T12:00:00Z',
        yes_token_id=token,
        no_token_id='NO_REG',
        fees_enabled=True,
        fee_rate=0.05,
    )


def test_terminal_one_dollar_quote_cannot_create_denver_style_signal(tmp_path):
    import asyncio
    from main import MarketInfo, TopOfBook

    db = Database(str(tmp_path / 'terminal.db'))
    m = _minimal_market(MarketInfo)
    db.upsert_market(m)

    bot = WeatherPaperBot.__new__(WeatherPaperBot)
    bot.db = db
    bot.tops = {}
    bot.markets_by_yes = {m.yes_token_id: m}
    bot.closed_market_ids = set()
    bot.last_asks = {m.yes_token_id: 0.001}
    bot.trading_enabled = True
    bot.signal_keys = set()
    bot.pending_crossings = set()
    bot.pending_stop_losses = set()

    asyncio.run(bot.handle_top(m.yes_token_id, 0.50, 1.0, 1234567890))
    count = db.conn.execute('SELECT COUNT(*) AS n FROM signals').fetchone()['n']
    assert count == 0


def test_explicitly_resolved_market_cannot_create_signal(tmp_path):
    import asyncio
    from main import MarketInfo

    db = Database(str(tmp_path / 'closed.db'))
    m = _minimal_market(MarketInfo, market_id='m_closed', token='YES_CLOSED')
    db.upsert_market(m)

    bot = WeatherPaperBot.__new__(WeatherPaperBot)
    bot.db = db
    bot.tops = {}
    bot.markets_by_yes = {m.yes_token_id: m}
    bot.closed_market_ids = {m.market_id}
    bot.last_asks = {m.yes_token_id: 0.78}
    bot.trading_enabled = True
    bot.signal_keys = set()
    bot.pending_crossings = set()
    bot.pending_stop_losses = set()

    asyncio.run(bot.handle_top(m.yes_token_id, 0.89, 0.90, 1234567890))
    count = db.conn.execute('SELECT COUNT(*) AS n FROM signals').fetchone()['n']
    assert count == 0


def test_market_status_rejects_not_accepting_orders():
    import asyncio
    from main import MarketInfo

    m = _minimal_market(MarketInfo, market_id='m_status', token='YES_STATUS')
    bot = WeatherPaperBot.__new__(WeatherPaperBot)
    bot.closed_market_ids = set()

    async def fake_gamma_get(path, params=None):
        assert path == '/markets/m_status'
        return {'active': True, 'closed': False, 'acceptingOrders': False}

    bot.gamma_get = fake_gamma_get
    assert asyncio.run(bot.market_accepting_orders(m)) is False


def test_entry_liquidity_window_rejects_far_away_depth():
    asks = [
        {"price": 0.79, "size": 0.5 / 0.79},  # only $0.50 near the best ask
        {"price": 0.80, "size": 1.0 / 0.80},  # another $1.00 within 2 cents
        {"price": 0.95, "size": 100.0},       # lots of depth, but far too expensive
    ]
    eligible, best, max_price, notional = WeatherPaperBot.entry_liquidity_window(asks, 0.02)
    assert math.isclose(best, 0.79)
    assert math.isclose(max_price, 0.81)
    assert len(eligible) == 2
    assert math.isclose(notional, 1.5, rel_tol=1e-9)
    fill = WeatherPaperBot.simulate_market_buy(eligible, 5.0)
    assert fill.full_fill_ratio < 0.999


def test_entry_liquidity_window_allows_full_five_dollars_near_best_ask():
    asks = [
        {"price": 0.79, "size": 2.0},
        {"price": 0.80, "size": 5.0},
        {"price": 0.95, "size": 100.0},
    ]
    eligible, best, max_price, notional = WeatherPaperBot.entry_liquidity_window(asks, 0.02)
    assert best == 0.79
    assert max_price == 0.81
    assert notional >= 5.0
    fill = WeatherPaperBot.simulate_market_buy(eligible, 5.0)
    assert math.isclose(fill.filled_notional, 5.0, rel_tol=1e-9)
    assert max(x["price"] for x in fill.levels) <= 0.81 + 1e-12
