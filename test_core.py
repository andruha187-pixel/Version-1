import math

from main import FillResult, WeatherPaperBot, parse_jsonish, yes_token_from_market


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
